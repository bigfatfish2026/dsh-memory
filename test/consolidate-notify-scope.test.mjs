/**
 * 压缩前整理推送：群聊不发、私聊照旧。
 *
 * 判据走 dsh-qqbot 的 `qqbot/prefs` 只读出口：payload().sessionIds 是
 * `sessionKey → sessionId`，key 第三段是 scope（'group' / 'c2c'）。
 *
 * 四条：
 *  ① 群会话：整理照跑、不调 notify.sh，日志留一行「群会话…不推 QQ」；
 *  ② 私聊：照旧调 notify.sh，消息里带「压缩前整理记忆」；
 *  ③ 判据缺失（prefs 里查不到这个 sessionId）：不调 notify，日志写「判据缺失，跳过推送」；
 *  ④ 无 sessionId（后台轮）：2026-11-14 起一律不触发整理，只写一行跳过日志。
 *     推送里那条「无 QQ 会话」分支只留给手动工具（不经 shouldRun 的那条路）。
 *
 * 全程 tmpdir：不碰真库、不碰 ~/.dsh-qqbot、不真发 QQ。
 * notify.sh 换成一个往标记文件写参数的临时脚本，用来判断到底调没调。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { setupConsolidate, qqScopeOf } from '../lib/consolidate.js';

function tempCfg() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'consolidate-notify-scope-'));
  const marker = path.join(dir, 'notify.txt');
  const script = path.join(dir, 'notify.sh');
  fs.writeFileSync(script, `#!/bin/sh\nprintf '%s' "$1" >> '${marker}'\n`, { mode: 0o755 });
  return {
    dir,
    marker,
    cfg: {
      consolidateEnabled: true,
      consolidateOnlyMain: false, // 这组用例只测推送判据，不掺子会话类判据
      triggerTokens: 1000,
      growthTokens: 0,
      maxHistoryChars: 600000,
      consolidateTools: false, // 关工具：一轮就出 JSON
      consolidateLogPath: path.join(dir, 'consolidate.log'),
      notifyScript: script,
    },
  };
}

/** estimateTokens 按 2 字符/token 算，2000 字符 = 1000 token，刚好过触发线。 */
function bigMessages() {
  return [{ role: 'user', content: 'x'.repeat(2000) }];
}

function jsonStream() {
  return (async function* () {
    yield { type: 'text-delta', text: '[{"text":"推送判据测试条目","type":"world"}]' };
    yield { type: 'finish', reason: { kind: 'stop' } };
  })();
}

/** 假 ctx：只给 sessions/qqbot-preferences 两条 get，其余服务一律没有。 */
function fakeHarness(prefsPayload) {
  const handlers = new Map();
  const ctx = {
    get: (name) => {
      if (name !== 'qqbot/prefs') return undefined;
      if (prefsPayload === undefined) return undefined;
      return { payload: () => prefsPayload };
    },
    on: (event, handler) => handlers.set(event, handler),
    tools: { register: () => {} },
    llm: { stream: () => jsonStream() },
  };
  return { ctx, handlers };
}

const passthrough = async function* () {
  yield { type: 'text-delta', text: '主回答' };
};

function readLog(cfg) {
  try {
    return fs.readFileSync(cfg.consolidateLogPath, 'utf8');
  } catch {
    return '';
  }
}

/** 跑一轮完整的「钩子 → 后台整理 → 落库 → 推送判定」。 */
async function runConsolidate({ cfg, prefsPayload, sessionId }) {
  const saved = [];
  const store = {
    listTexts: () => [],
    save: async (text) => {
      saved.push(text);
      return { text };
    },
  };
  const { ctx, handlers } = fakeHarness(prefsPayload);
  setupConsolidate(ctx, store, cfg, {});
  const handler = handlers.get('llm/stream');
  const options = { messages: bigMessages() };
  if (sessionId !== undefined) options.sessionId = sessionId;
  const stream = handler(options, passthrough);
  for await (const _ of stream) {
    /* 原请求照常放行 */
  }
  // 2026-11-14：无 sessionId 的请求不再触发整理，钩子返回原请求自己的流（没有 pending）。
  if (!stream.pending) return { triggered: false, result: undefined, saved };
  const result = await stream.pending;
  return { triggered: true, result, saved };
}

async function waitForFile(file, timeoutMs = 3000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (fs.existsSync(file)) return fs.readFileSync(file, 'utf8');
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return '';
}

function prefsWith(scope, sessionId) {
  return { overrides: {}, presets: {}, sessionIds: { [`qqbot:1905918172:${scope}:PEERID`]: sessionId } };
}

test('qqScopeOf：按 prefs 的 sessionKey 第三段认 group / c2c，查不到给 null', () => {
  const ctx = (payload) => ({ get: () => ({ payload: () => payload }) });
  assert.equal(qqScopeOf(ctx(prefsWith('group', 'session-a')), 'session-a'), 'group');
  assert.equal(qqScopeOf(ctx(prefsWith('c2c', 'session-b')), 'session-b'), 'c2c');
  assert.equal(qqScopeOf(ctx(prefsWith('c2c', 'session-b')), 'session-c'), null, '查不到 → null');
  assert.equal(qqScopeOf(ctx(undefined), 'session-b'), null, '出口不在 → null');
  assert.equal(qqScopeOf(ctx({ overrides: {}, presets: {} }), 'session-b'), null, 'sessionIds 字段缺失 → null');
  assert.equal(qqScopeOf(ctx(prefsWith('c2c', 'session-b')), ''), null, '无 sessionId → null');
});

test('群会话：整理照跑，但不调 notify.sh，日志有一行', async () => {
  const { cfg, marker } = tempCfg();
  const sessionId = 'session-group-0001';
  const { result, saved } = await runConsolidate({
    cfg,
    prefsPayload: prefsWith('group', sessionId),
    sessionId,
  });

  assert.equal(result.saved, 1, '群会话的整理本身要照跑');
  assert.equal(saved.length, 1);
  await new Promise((resolve) => setTimeout(resolve, 300)); // 给可能误发的 spawn 留时间
  assert.equal(fs.existsSync(marker), false, '群会话不该调 notify.sh');
  assert.match(readLog(cfg), /群会话，整理新增 1 条，不推 QQ/);
});

test('私聊：照旧调 notify.sh，消息里带整理文案', async () => {
  const { cfg, marker } = tempCfg();
  const sessionId = 'session-c2c-0001';
  const { result } = await runConsolidate({
    cfg,
    prefsPayload: prefsWith('c2c', sessionId),
    sessionId,
  });

  assert.equal(result.saved, 1);
  const text = await waitForFile(marker);
  assert.match(text, /压缩前整理记忆/);
  assert.match(text, /新增 1 条/);
  assert.doesNotMatch(readLog(cfg), /不推 QQ|判据缺失/);
});

test('判据缺失：prefs 里没有这个会话 → 不调 notify，日志写明跳过', async () => {
  const { cfg, marker } = tempCfg();
  const { result } = await runConsolidate({
    cfg,
    prefsPayload: { overrides: {}, presets: {}, sessionIds: {} },
    sessionId: 'session-legacy-9999',
  });

  assert.equal(result.saved, 1);
  await new Promise((resolve) => setTimeout(resolve, 300));
  assert.equal(fs.existsSync(marker), false, '判据缺失时保守不发');
  assert.match(readLog(cfg), /判据缺失，跳过推送/);
});

test('无 sessionId 的后台轮：2026-11-14 起一律不触发整理，也不会推 QQ', async () => {
  const { cfg, marker } = tempCfg();
  const { triggered, saved } = await runConsolidate({
    cfg,
    prefsPayload: undefined,
    sessionId: undefined,
  });

  // 2026-11-14：没有真实上下文可整理，估算不可信，直接挡在 shouldRun。
  // 推送里那条 !sid 分支只留给手动工具（它不经 shouldRun）。
  assert.equal(triggered, false, '无 sessionId 请求不该触发整理');
  assert.equal(saved.length, 0);
  await new Promise((resolve) => setTimeout(resolve, 300));
  assert.equal(fs.existsSync(marker), false, '无会话时不推 QQ');
  assert.match(readLog(cfg), /跳过整理：无 sessionId 请求/);
});
