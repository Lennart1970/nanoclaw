import fs from 'fs';
import path from 'path';

import { OneCLI } from '@onecli-sh/sdk';

import {
  ASSISTANT_NAME,
  DEFAULT_TRIGGER,
  getTriggerPattern,
  GROUPS_DIR,
  IDLE_TIMEOUT,
  MAX_MESSAGES_PER_PROMPT,
  ONECLI_URL,
  POLL_INTERVAL,
  TIMEZONE,
} from './config.js';
import './channels/index.js';
import {
  getChannelFactory,
  getRegisteredChannelNames,
} from './channels/registry.js';
import {
  ContainerOutput,
  runContainerAgent,
  writeGroupsSnapshot,
  writeTasksSnapshot,
} from './container-runner.js';
import {
  cleanupOrphans,
  ensureContainerRuntimeRunning,
} from './container-runtime.js';
import {
  addChannelLink,
  getAllChats,
  getAllChannelLinks,
  getAllRegisteredGroups,
  getAllSessions,
  deleteSession,
  getAllTasks,
  getLastBotMessageTimestamp,
  getMessagesSinceMulti,
  getNewMessages,
  getRouterState,
  initDatabase,
  removeChannelLink,
  setRegisteredGroup,
  setRouterState,
  setSession,
  storeChatMetadata,
  storeMessage,
} from './db.js';
import { GroupQueue } from './group-queue.js';
import { resolveGroupFolderPath } from './group-folder.js';
import { startIpcWatcher } from './ipc.js';
import { findChannel, formatMessages, formatOutbound } from './router.js';
import {
  restoreRemoteControl,
  startRemoteControl,
  stopRemoteControl,
} from './remote-control.js';
import {
  isSenderAllowed,
  isTriggerAllowed,
  loadSenderAllowlist,
  shouldDropMessage,
} from './sender-allowlist.js';
import { startSessionCleanup } from './session-cleanup.js';
import { startSchedulerLoop } from './task-scheduler.js';
import { Channel, NewMessage, RegisteredGroup } from './types.js';
import { logger } from './logger.js';

// Re-export for backwards compatibility during refactor
export { escapeXml, formatMessages } from './router.js';

let lastTimestamp = '';
let sessions: Record<string, string> = {};
let registeredGroups: Record<string, RegisteredGroup> = {};
let lastAgentTimestamp: Record<string, string> = {};
let messageLoopRunning = false;

// Channel mirroring state: primary_jid → linked_jid[], and reverse lookup
let channelLinksMap: Record<string, string[]> = {};
let linkedToPrimary: Record<string, string> = {};

const channels: Channel[] = [];
const queue = new GroupQueue();

const onecli = new OneCLI({ url: ONECLI_URL });

function ensureOneCLIAgent(jid: string, group: RegisteredGroup): void {
  if (group.isMain) return;
  const identifier = group.folder.toLowerCase().replace(/_/g, '-');
  onecli.ensureAgent({ name: group.name, identifier }).then(
    (res) => {
      logger.info(
        { jid, identifier, created: res.created },
        'OneCLI agent ensured',
      );
    },
    (err) => {
      logger.debug(
        { jid, identifier, err: String(err) },
        'OneCLI agent ensure skipped',
      );
    },
  );
}

function loadState(): void {
  lastTimestamp = getRouterState('last_timestamp') || '';
  const agentTs = getRouterState('last_agent_timestamp');
  try {
    lastAgentTimestamp = agentTs ? JSON.parse(agentTs) : {};
  } catch {
    logger.warn('Corrupted last_agent_timestamp in DB, resetting');
    lastAgentTimestamp = {};
  }
  sessions = getAllSessions();
  registeredGroups = getAllRegisteredGroups();
  loadChannelLinks();
  logger.info(
    { groupCount: Object.keys(registeredGroups).length },
    'State loaded',
  );
}

function loadChannelLinks(): void {
  channelLinksMap = {};
  linkedToPrimary = {};
  for (const link of getAllChannelLinks()) {
    if (!channelLinksMap[link.primary_jid]) {
      channelLinksMap[link.primary_jid] = [];
    }
    channelLinksMap[link.primary_jid].push(link.linked_jid);
    linkedToPrimary[link.linked_jid] = link.primary_jid;
  }
  const linkCount = Object.values(channelLinksMap).reduce(
    (n, arr) => n + arr.length,
    0,
  );
  if (linkCount > 0) {
    logger.info({ linkCount }, 'Channel links loaded');
  }
}

/**
 * Returns registered groups augmented with linked channel JIDs.
 * Linked JIDs map to the primary group's config so channels accept messages for them.
 */
function getEffectiveRegisteredGroups(): Record<string, RegisteredGroup> {
  const effective = { ...registeredGroups };
  for (const [primaryJid, linkedJids] of Object.entries(channelLinksMap)) {
    const primaryGroup = registeredGroups[primaryJid];
    if (!primaryGroup) continue;
    for (const linkedJid of linkedJids) {
      if (!effective[linkedJid]) {
        effective[linkedJid] = { ...primaryGroup };
      }
    }
  }
  return effective;
}

/**
 * Send a message to the primary channel and all linked mirror channels.
 */
async function sendToGroup(primaryJid: string, text: string): Promise<void> {
  const ch = findChannel(channels, primaryJid);
  if (ch?.isConnected()) {
    await ch.sendMessage(primaryJid, text);
  }
  for (const linkedJid of channelLinksMap[primaryJid] || []) {
    const lc = findChannel(channels, linkedJid);
    if (lc?.isConnected()) {
      try {
        await lc.sendMessage(linkedJid, text);
      } catch (err) {
        logger.warn(
          { linkedJid, err },
          'Failed to mirror message to linked channel',
        );
      }
    }
  }
}

/**
 * Resolve a JID to its primary group JID if it's a linked channel,
 * otherwise return it unchanged.
 */
function resolvePrimaryJid(jid: string): string {
  return linkedToPrimary[jid] || jid;
}

/**
 * Deterministic message bridge: forwards a user message to all linked channels
 * without invoking the agent. Zero tokens, near-instant delivery.
 */
function bridgeMessageToLinkedChannels(
  sourceJid: string,
  msg: NewMessage,
): void {
  // Find which group this JID belongs to and all its linked JIDs
  const primaryJid = resolvePrimaryJid(sourceJid);
  const linkedJids = channelLinksMap[primaryJid];
  if (!linkedJids?.length && sourceJid === primaryJid) return;

  // Collect all JIDs in this group except the source
  const targetJids: string[] = [];
  if (sourceJid !== primaryJid) {
    targetJids.push(primaryJid);
  }
  for (const jid of linkedJids || []) {
    if (jid !== sourceJid) {
      targetJids.push(jid);
    }
  }
  if (targetJids.length === 0) return;

  // Determine source channel name for attribution
  const sourceChannel = findChannel(channels, sourceJid);
  const tag = sourceChannel?.name || 'unknown';
  const bridgedText = `[${tag}] ${msg.sender_name}: ${msg.content}`;

  for (const targetJid of targetJids) {
    const ch = findChannel(channels, targetJid);
    if (ch?.isConnected()) {
      ch.sendMessage(targetJid, bridgedText).catch((err) =>
        logger.warn(
          { sourceJid, targetJid, err },
          'Failed to bridge message to linked channel',
        ),
      );
    }
  }
}

export function linkChannel(primaryJid: string, linkedJid: string): void {
  addChannelLink(primaryJid, linkedJid);
  loadChannelLinks();
  logger.info({ primaryJid, linkedJid }, 'Channel linked');
}

export function unlinkChannel(primaryJid: string, linkedJid: string): void {
  removeChannelLink(primaryJid, linkedJid);
  loadChannelLinks();
  logger.info({ primaryJid, linkedJid }, 'Channel unlinked');
}

/**
 * Return the message cursor for a group, recovering from the last bot reply
 * if lastAgentTimestamp is missing (new group, corrupted state, restart).
 */
function getOrRecoverCursor(chatJid: string): string {
  const existing = lastAgentTimestamp[chatJid];
  if (existing) return existing;

  const botTs = getLastBotMessageTimestamp(chatJid, ASSISTANT_NAME);
  if (botTs) {
    logger.info(
      { chatJid, recoveredFrom: botTs },
      'Recovered message cursor from last bot reply',
    );
    lastAgentTimestamp[chatJid] = botTs;
    saveState();
    return botTs;
  }
  return '';
}

function saveState(): void {
  setRouterState('last_timestamp', lastTimestamp);
  setRouterState('last_agent_timestamp', JSON.stringify(lastAgentTimestamp));
}

function registerGroup(jid: string, group: RegisteredGroup): void {
  let groupDir: string;
  try {
    groupDir = resolveGroupFolderPath(group.folder);
  } catch (err) {
    logger.warn(
      { jid, folder: group.folder, err },
      'Rejecting group registration with invalid folder',
    );
    return;
  }

  registeredGroups[jid] = group;
  setRegisteredGroup(jid, group);

  // Create group folder
  fs.mkdirSync(path.join(groupDir, 'logs'), { recursive: true });

  // Copy CLAUDE.md template into the new group folder so agents have
  // identity and instructions from the first run.  (Fixes #1391)
  const groupMdFile = path.join(groupDir, 'CLAUDE.md');
  if (!fs.existsSync(groupMdFile)) {
    const templateFile = path.join(
      GROUPS_DIR,
      group.isMain ? 'main' : 'global',
      'CLAUDE.md',
    );
    if (fs.existsSync(templateFile)) {
      let content = fs.readFileSync(templateFile, 'utf-8');
      if (ASSISTANT_NAME !== 'Andy') {
        content = content.replace(/^# Andy$/m, `# ${ASSISTANT_NAME}`);
        content = content.replace(/You are Andy/g, `You are ${ASSISTANT_NAME}`);
      }
      fs.writeFileSync(groupMdFile, content);
      logger.info({ folder: group.folder }, 'Created CLAUDE.md from template');
    }
  }

  // Ensure a corresponding OneCLI agent exists (best-effort, non-blocking)
  ensureOneCLIAgent(jid, group);

  logger.info(
    { jid, name: group.name, folder: group.folder },
    'Group registered',
  );
}

/**
 * Get available groups list for the agent.
 * Returns groups ordered by most recent activity.
 */
export function getAvailableGroups(): import('./container-runner.js').AvailableGroup[] {
  const chats = getAllChats();
  const registeredJids = new Set(Object.keys(registeredGroups));

  return chats
    .filter((c) => c.jid !== '__group_sync__' && c.is_group)
    .map((c) => ({
      jid: c.jid,
      name: c.name,
      lastActivity: c.last_message_time,
      isRegistered: registeredJids.has(c.jid),
    }));
}

/** @internal - exported for testing */
export function _setRegisteredGroups(
  groups: Record<string, RegisteredGroup>,
): void {
  registeredGroups = groups;
}

/**
 * Process all pending messages for a group.
 * Called by the GroupQueue when it's this group's turn.
 */
async function processGroupMessages(chatJid: string): Promise<boolean> {
  // Resolve to primary JID if this is a linked channel
  const primaryJid = resolvePrimaryJid(chatJid);
  const group = registeredGroups[primaryJid];
  if (!group) return true;

  const channel = findChannel(channels, primaryJid);
  if (!channel) {
    logger.warn(
      { chatJid: primaryJid },
      'No channel owns JID, skipping messages',
    );
    return true;
  }

  const isMainGroup = group.isMain === true;

  // Fetch messages from primary and all linked channels
  const allJids = [primaryJid, ...(channelLinksMap[primaryJid] || [])];
  const cursor = getOrRecoverCursor(primaryJid);
  const missedMessages = getMessagesSinceMulti(
    allJids,
    cursor,
    ASSISTANT_NAME,
    MAX_MESSAGES_PER_PROMPT,
  );

  if (missedMessages.length === 0) return true;

  // For non-main groups, check if trigger is required and present
  if (!isMainGroup && group.requiresTrigger !== false) {
    const triggerPattern = getTriggerPattern(group.trigger);
    const allowlistCfg = loadSenderAllowlist();
    const hasTrigger = missedMessages.some(
      (m) =>
        triggerPattern.test(m.content.trim()) &&
        (m.is_from_me || isTriggerAllowed(primaryJid, m.sender, allowlistCfg)),
    );
    if (!hasTrigger) return true;
  }

  // Add channel attribution for messages from linked channels
  if (channelLinksMap[primaryJid]?.length) {
    addChannelAttribution(missedMessages, primaryJid);
  }

  const prompt = formatMessages(missedMessages, TIMEZONE);

  // Advance cursor so the piping path in startMessageLoop won't re-fetch
  // these messages. Save the old cursor so we can roll back on error.
  const previousCursor = lastAgentTimestamp[primaryJid] || '';
  lastAgentTimestamp[primaryJid] =
    missedMessages[missedMessages.length - 1].timestamp;
  saveState();

  logger.info(
    { group: group.name, messageCount: missedMessages.length },
    'Processing messages',
  );

  // Track idle timer for closing stdin when agent is idle
  let idleTimer: ReturnType<typeof setTimeout> | null = null;

  const resetIdleTimer = () => {
    if (idleTimer) clearTimeout(idleTimer);
    idleTimer = setTimeout(() => {
      logger.debug(
        { group: group.name },
        'Idle timeout, closing container stdin',
      );
      queue.closeStdin(primaryJid);
    }, IDLE_TIMEOUT);
  };

  setGroupTyping(primaryJid, true);
  let hadError = false;
  let outputSentToUser = false;

  const output = await runAgent(group, prompt, primaryJid, async (result) => {
    // Streaming output callback — called for each agent result
    if (result.result) {
      const raw =
        typeof result.result === 'string'
          ? result.result
          : JSON.stringify(result.result);
      // Strip <internal>...</internal> blocks — agent uses these for internal reasoning
      const text = raw.replace(/<internal>[\s\S]*?<\/internal>/g, '').trim();
      logger.info({ group: group.name }, `Agent output: ${raw.length} chars`);
      if (text) {
        await sendToGroup(primaryJid, text);
        outputSentToUser = true;
      }
      // Only reset idle timer on actual results, not session-update markers (result: null)
      resetIdleTimer();
    }

    if (result.status === 'success') {
      queue.notifyIdle(primaryJid);
    }

    if (result.status === 'error') {
      hadError = true;
    }
  });

  setGroupTyping(primaryJid, false);
  if (idleTimer) clearTimeout(idleTimer);

  if (output === 'error' || hadError) {
    // If we already sent output to the user, don't roll back the cursor —
    // the user got their response and re-processing would send duplicates.
    if (outputSentToUser) {
      logger.warn(
        { group: group.name },
        'Agent error after output was sent, skipping cursor rollback to prevent duplicates',
      );
      return true;
    }
    // Roll back cursor so retries can re-process these messages
    lastAgentTimestamp[primaryJid] = previousCursor;
    saveState();
    logger.warn(
      { group: group.name },
      'Agent error, rolled back message cursor for retry',
    );
    return false;
  }

  return true;
}

async function runAgent(
  group: RegisteredGroup,
  prompt: string,
  chatJid: string,
  onOutput?: (output: ContainerOutput) => Promise<void>,
): Promise<'success' | 'error'> {
  const isMain = group.isMain === true;
  const sessionId = sessions[group.folder];

  // Update tasks snapshot for container to read (filtered by group)
  const tasks = getAllTasks();
  writeTasksSnapshot(
    group.folder,
    isMain,
    tasks.map((t) => ({
      id: t.id,
      groupFolder: t.group_folder,
      prompt: t.prompt,
      script: t.script || undefined,
      schedule_type: t.schedule_type,
      schedule_value: t.schedule_value,
      status: t.status,
      next_run: t.next_run,
    })),
  );

  // Update available groups snapshot (main group only can see all groups)
  const availableGroups = getAvailableGroups();
  writeGroupsSnapshot(
    group.folder,
    isMain,
    availableGroups,
    new Set(Object.keys(registeredGroups)),
  );

  // Wrap onOutput to track session ID from streamed results
  const wrappedOnOutput = onOutput
    ? async (output: ContainerOutput) => {
        if (output.newSessionId) {
          sessions[group.folder] = output.newSessionId;
          setSession(group.folder, output.newSessionId);
        }
        await onOutput(output);
      }
    : undefined;

  try {
    const output = await runContainerAgent(
      group,
      {
        prompt,
        sessionId,
        groupFolder: group.folder,
        chatJid,
        isMain,
        assistantName: ASSISTANT_NAME,
      },
      (proc, containerName) =>
        queue.registerProcess(chatJid, proc, containerName, group.folder),
      wrappedOnOutput,
    );

    if (output.newSessionId) {
      sessions[group.folder] = output.newSessionId;
      setSession(group.folder, output.newSessionId);
    }

    if (output.status === 'error') {
      // Detect stale/corrupt session — clear it so the next retry starts fresh.
      // The session .jsonl can go missing after a crash mid-write, manual
      // deletion, or disk-full. The existing backoff in group-queue.ts
      // handles the retry; we just need to remove the broken session ID.
      const isStaleSession =
        sessionId &&
        output.error &&
        /no conversation found|ENOENT.*\.jsonl|session.*not found/i.test(
          output.error,
        );

      if (isStaleSession) {
        logger.warn(
          { group: group.name, staleSessionId: sessionId, error: output.error },
          'Stale session detected — clearing for next retry',
        );
        delete sessions[group.folder];
        deleteSession(group.folder);
      }

      logger.error(
        { group: group.name, error: output.error },
        'Container agent error',
      );
      return 'error';
    }

    return 'success';
  } catch (err) {
    logger.error({ group: group.name, err }, 'Agent error');
    return 'error';
  }
}

async function startMessageLoop(): Promise<void> {
  if (messageLoopRunning) {
    logger.debug('Message loop already running, skipping duplicate start');
    return;
  }
  messageLoopRunning = true;

  logger.info(`NanoClaw running (default trigger: ${DEFAULT_TRIGGER})`);

  while (true) {
    try {
      const effectiveGroups = getEffectiveRegisteredGroups();
      const jids = Object.keys(effectiveGroups);
      const { messages, newTimestamp } = getNewMessages(
        jids,
        lastTimestamp,
        ASSISTANT_NAME,
      );

      if (messages.length > 0) {
        logger.info({ count: messages.length }, 'New messages');

        // Advance the "seen" cursor for all messages immediately
        lastTimestamp = newTimestamp;
        saveState();

        // Group by chat_jid, then merge linked JIDs into their primary group
        const messagesByJid = new Map<string, NewMessage[]>();
        for (const msg of messages) {
          const existing = messagesByJid.get(msg.chat_jid);
          if (existing) {
            existing.push(msg);
          } else {
            messagesByJid.set(msg.chat_jid, [msg]);
          }
        }

        // Merge linked channel messages into primary group buckets
        const messagesByGroup = new Map<string, NewMessage[]>();
        for (const [chatJid, msgs] of messagesByJid) {
          const primaryJid = resolvePrimaryJid(chatJid);
          const existing = messagesByGroup.get(primaryJid) || [];
          existing.push(...msgs);
          messagesByGroup.set(primaryJid, existing);
        }

        for (const [primaryJid, groupMessages] of messagesByGroup) {
          const group = registeredGroups[primaryJid];
          if (!group) continue;

          const channel = findChannel(channels, primaryJid);
          if (!channel) {
            logger.warn(
              { chatJid: primaryJid },
              'No channel owns JID, skipping messages',
            );
            continue;
          }

          const isMainGroup = group.isMain === true;
          const needsTrigger = !isMainGroup && group.requiresTrigger !== false;

          // For non-main groups, only act on trigger messages.
          // Non-trigger messages accumulate in DB and get pulled as
          // context when a trigger eventually arrives.
          if (needsTrigger) {
            const triggerPattern = getTriggerPattern(group.trigger);
            const allowlistCfg = loadSenderAllowlist();
            const hasTrigger = groupMessages.some(
              (m) =>
                triggerPattern.test(m.content.trim()) &&
                (m.is_from_me ||
                  isTriggerAllowed(primaryJid, m.sender, allowlistCfg)),
            );
            if (!hasTrigger) continue;
          }

          // Pull all messages since lastAgentTimestamp from primary
          // and all linked channels so accumulated context is included.
          const allJids = [primaryJid, ...(channelLinksMap[primaryJid] || [])];
          const cursor = getOrRecoverCursor(primaryJid);
          const allPending = getMessagesSinceMulti(
            allJids,
            cursor,
            ASSISTANT_NAME,
            MAX_MESSAGES_PER_PROMPT,
          );

          const messagesToSend =
            allPending.length > 0 ? allPending : groupMessages;

          // Add channel attribution for messages from linked channels
          const linkedJids = channelLinksMap[primaryJid];
          if (linkedJids?.length) {
            addChannelAttribution(messagesToSend, primaryJid);
          }

          const formatted = formatMessages(messagesToSend, TIMEZONE);

          if (queue.sendMessage(primaryJid, formatted)) {
            logger.debug(
              { chatJid: primaryJid, count: messagesToSend.length },
              'Piped messages to active container',
            );
            lastAgentTimestamp[primaryJid] =
              messagesToSend[messagesToSend.length - 1].timestamp;
            saveState();
            // Show typing indicator on primary and linked channels
            setGroupTyping(primaryJid, true);
          } else {
            // No active container — enqueue for a new one
            queue.enqueueMessageCheck(primaryJid);
          }
        }
      }
    } catch (err) {
      logger.error({ err }, 'Error in message loop');
    }
    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL));
  }
}

/**
 * Tag sender names with channel attribution for messages from linked channels.
 * Only mutates sender_name for non-bot messages from a different channel.
 */
function addChannelAttribution(
  messages: NewMessage[],
  primaryJid: string,
): void {
  for (const msg of messages) {
    if (msg.chat_jid !== primaryJid && !msg.is_bot_message && !msg.is_from_me) {
      const sourceChannel = findChannel(channels, msg.chat_jid);
      if (sourceChannel) {
        msg.sender_name = `[${sourceChannel.name}] ${msg.sender_name}`;
      }
    }
  }
}

/**
 * Set typing indicator on primary channel and all linked mirror channels.
 */
function setGroupTyping(primaryJid: string, isTyping: boolean): void {
  const ch = findChannel(channels, primaryJid);
  ch?.setTyping?.(primaryJid, isTyping)?.catch((err) =>
    logger.warn({ chatJid: primaryJid, err }, 'Failed to set typing indicator'),
  );
  for (const linkedJid of channelLinksMap[primaryJid] || []) {
    const lc = findChannel(channels, linkedJid);
    lc?.setTyping?.(linkedJid, isTyping)?.catch((err) =>
      logger.warn(
        { chatJid: linkedJid, err },
        'Failed to set typing on linked channel',
      ),
    );
  }
}

/**
 * Startup recovery: check for unprocessed messages in registered groups.
 * Handles crash between advancing lastTimestamp and processing messages.
 */
function recoverPendingMessages(): void {
  for (const [chatJid, group] of Object.entries(registeredGroups)) {
    const allJids = [chatJid, ...(channelLinksMap[chatJid] || [])];
    const pending = getMessagesSinceMulti(
      allJids,
      getOrRecoverCursor(chatJid),
      ASSISTANT_NAME,
      MAX_MESSAGES_PER_PROMPT,
    );
    if (pending.length > 0) {
      logger.info(
        { group: group.name, pendingCount: pending.length },
        'Recovery: found unprocessed messages',
      );
      queue.enqueueMessageCheck(chatJid);
    }
  }
}

function ensureContainerSystemRunning(): void {
  ensureContainerRuntimeRunning();
  cleanupOrphans();
}

async function main(): Promise<void> {
  ensureContainerSystemRunning();
  initDatabase();
  logger.info('Database initialized');
  loadState();

  // Ensure OneCLI agents exist for all registered groups.
  // Recovers from missed creates (e.g. OneCLI was down at registration time).
  for (const [jid, group] of Object.entries(registeredGroups)) {
    ensureOneCLIAgent(jid, group);
  }

  restoreRemoteControl();

  // Graceful shutdown handlers
  const shutdown = async (signal: string) => {
    logger.info({ signal }, 'Shutdown signal received');
    await queue.shutdown(10000);
    for (const ch of channels) await ch.disconnect();
    process.exit(0);
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  // Handle /remote-control and /remote-control-end commands
  async function handleRemoteControl(
    command: string,
    chatJid: string,
    msg: NewMessage,
  ): Promise<void> {
    const group = registeredGroups[chatJid];
    if (!group?.isMain) {
      logger.warn(
        { chatJid, sender: msg.sender },
        'Remote control rejected: not main group',
      );
      return;
    }

    const channel = findChannel(channels, chatJid);
    if (!channel) return;

    if (command === '/remote-control') {
      const result = await startRemoteControl(
        msg.sender,
        chatJid,
        process.cwd(),
      );
      if (result.ok) {
        await channel.sendMessage(chatJid, result.url);
      } else {
        await channel.sendMessage(
          chatJid,
          `Remote Control failed: ${result.error}`,
        );
      }
    } else {
      const result = stopRemoteControl();
      if (result.ok) {
        await channel.sendMessage(chatJid, 'Remote Control session ended.');
      } else {
        await channel.sendMessage(chatJid, result.error);
      }
    }
  }

  // Channel callbacks (shared by all channels)
  const channelOpts = {
    onMessage: (chatJid: string, msg: NewMessage) => {
      // Remote control commands — intercept before storage
      const trimmed = msg.content.trim();
      if (trimmed === '/remote-control' || trimmed === '/remote-control-end') {
        handleRemoteControl(trimmed, chatJid, msg).catch((err) =>
          logger.error({ err, chatJid }, 'Remote control command error'),
        );
        return;
      }

      // For linked channels, apply sender allowlist using the primary group's JID
      const effectiveJid = resolvePrimaryJid(chatJid);
      const effectiveGroup =
        registeredGroups[effectiveJid] || registeredGroups[chatJid];

      // Sender allowlist drop mode: discard messages from denied senders before storing
      if (!msg.is_from_me && !msg.is_bot_message && effectiveGroup) {
        const cfg = loadSenderAllowlist();
        if (
          shouldDropMessage(effectiveJid, cfg) &&
          !isSenderAllowed(effectiveJid, msg.sender, cfg)
        ) {
          if (cfg.logDenied) {
            logger.debug(
              { chatJid, sender: msg.sender },
              'sender-allowlist: dropping message (drop mode)',
            );
          }
          return;
        }
      }
      storeMessage(msg);

      // Deterministic channel bridge: forward user messages to linked channels
      // instantly — no agent, no tokens, just code.
      if (!msg.is_from_me && !msg.is_bot_message) {
        bridgeMessageToLinkedChannels(chatJid, msg);
      }
    },
    onChatMetadata: (
      chatJid: string,
      timestamp: string,
      name?: string,
      channel?: string,
      isGroup?: boolean,
    ) => storeChatMetadata(chatJid, timestamp, name, channel, isGroup),
    registeredGroups: () => getEffectiveRegisteredGroups(),
  };

  // Create and connect all registered channels.
  // Each channel self-registers via the barrel import above.
  // Factories return null when credentials are missing, so unconfigured channels are skipped.
  for (const channelName of getRegisteredChannelNames()) {
    const factory = getChannelFactory(channelName)!;
    const channel = factory(channelOpts);
    if (!channel) {
      logger.warn(
        { channel: channelName },
        'Channel installed but credentials missing — skipping. Check .env or re-run the channel skill.',
      );
      continue;
    }
    channels.push(channel);
    await channel.connect();
  }
  if (channels.length === 0) {
    logger.fatal('No channels connected');
    process.exit(1);
  }

  // Start subsystems (independently of connection handler)
  startSchedulerLoop({
    registeredGroups: () => registeredGroups,
    getSessions: () => sessions,
    queue,
    onProcess: (groupJid, proc, containerName, groupFolder) =>
      queue.registerProcess(groupJid, proc, containerName, groupFolder),
    sendMessage: async (jid, rawText) => {
      const primaryJid = resolvePrimaryJid(jid);
      const channel = findChannel(channels, primaryJid);
      if (!channel) {
        logger.warn(
          { jid: primaryJid },
          'No channel owns JID, cannot send message',
        );
        return;
      }
      const text = formatOutbound(rawText);
      if (text) await sendToGroup(primaryJid, text);
    },
  });
  startIpcWatcher({
    sendMessage: async (jid, text) => {
      const primaryJid = resolvePrimaryJid(jid);
      await sendToGroup(primaryJid, text);
    },
    registeredGroups: () => registeredGroups,
    registerGroup,
    syncGroups: async (force: boolean) => {
      await Promise.all(
        channels
          .filter((ch) => ch.syncGroups)
          .map((ch) => ch.syncGroups!(force)),
      );
    },
    getAvailableGroups,
    writeGroupsSnapshot: (gf, im, ag, rj) =>
      writeGroupsSnapshot(gf, im, ag, rj),
    linkChannel,
    unlinkChannel,
    getChannelLinks: () => channelLinksMap,
    onTasksChanged: () => {
      const tasks = getAllTasks();
      const taskRows = tasks.map((t) => ({
        id: t.id,
        groupFolder: t.group_folder,
        prompt: t.prompt,
        script: t.script || undefined,
        schedule_type: t.schedule_type,
        schedule_value: t.schedule_value,
        status: t.status,
        next_run: t.next_run,
      }));
      for (const group of Object.values(registeredGroups)) {
        writeTasksSnapshot(group.folder, group.isMain === true, taskRows);
      }
    },
  });
  startSessionCleanup();
  queue.setProcessMessagesFn(processGroupMessages);
  recoverPendingMessages();
  startMessageLoop().catch((err) => {
    logger.fatal({ err }, 'Message loop crashed unexpectedly');
    process.exit(1);
  });
}

// Guard: only run when executed directly, not when imported by tests
const isDirectRun =
  process.argv[1] &&
  new URL(import.meta.url).pathname ===
    new URL(`file://${process.argv[1]}`).pathname;

if (isDirectRun) {
  main().catch((err) => {
    logger.error({ err }, 'Failed to start NanoClaw');
    process.exit(1);
  });
}
