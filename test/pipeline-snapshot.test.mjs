import assert from 'node:assert/strict';
import test from 'node:test';
import { Config, logPipelineSnapshot, pipelineSnapshotLine } from '../lib/index.js';

function snapshotFrom(config) {
  const lines = [];
  const line = logPipelineSnapshot({ logger: { info: (s) => lines.push(s) } }, config);
  assert.equal(lines.length, 1, '启动快照应该只打一行');
  assert.equal(lines[0], line, '返回值和日志行必须是同一串');
  return { line, lines };
}

test('快照行带三个开关与阈值', () => {
  const { line } = snapshotFrom(
    // 2026-11-14：ingestBudget / replayBudget 删了，旧配置键传进来也不报
    Config({ pipeline: { ingestAck: true, agentPipeline: false, writeGate: true, writeGateThreshold: 0.8, ingestBudget: 7, replayBudget: 3 } }),
  );
  assert.match(line, /^\[memory\] pipeline /);
  assert.match(line, /ingestAck=true/);
  assert.match(line, /agentPipeline=false/);
  assert.match(line, /writeGate=true/);
  assert.match(line, /threshold=0\.8/);
  assert.doesNotMatch(line, /ingestBudget|replayBudget/);
});

test('默认配置报 false 与默认阈值', () => {
  const { line } = snapshotFrom(Config({}));
  assert.match(line, /ingestAck=false/);
  assert.match(line, /agentPipeline=false/);
  assert.match(line, /writeGate=false/);
  assert.match(line, /threshold=0\.9/);
  assert.doesNotMatch(line, /ingestBudget|replayBudget/);
  assert.match(line, /store=(sqlite|jsonl)/);
  assert.match(line, / v=\d+\.\d+\.\d+/);
});

test('pipeline 整块缺失时也不抛，按默认值报', () => {
  const line = pipelineSnapshotLine({ storePath: '/nonexistent-memory-dir' }, { version: '9.9.9', store: 'jsonl' });
  assert.equal(
    line,
    '[memory] pipeline ingestAck=false agentPipeline=false writeGate=false threshold=0.9 store=jsonl v=9.9.9',
  );
});
