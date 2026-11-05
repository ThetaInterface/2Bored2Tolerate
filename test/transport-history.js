const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const fs = require('node:fs');
const { test } = require('node:test');
const Transport = require('../src/transport');
const mc = require('minecraft-protocol');

function fixture(t) {
  // Exercise transport independently of authentication and mineflayer plugins.
  const transport = Object.create(Transport.prototype);
  transport.client = { state: 'play', version: '1.21.4' };
  transport.maxBytes = 32;
  transport.registries = { test: {} };
  transport.configuration = new Map();
  transport.bot = { entity: { position: { x: 1, y: 80, z: 2 }, yaw: 0, pitch: 0 },
    clearControlStates() {}, physicsEnabled: true };
  transport.writeRaw = () => {};
  transport.resetHistory();
  const capture = (name, raw, data = {}) => transport.capture(data, { state: 'play', name }, raw);
  capture('login', Buffer.from('login'));
  capture('position', Buffer.from('old-relative-position'));
  t.after(() => transport.dispose());
  return { transport, capture };
}

function player() {
  const client = new EventEmitter();
  client.state = 'play';
  client.framer = new EventEmitter();
  Object.assign(client.framer, { writable: true, writableNeedDrain: false, writableLength: 0 });
  client.packets = [];
  client.writeRaw = raw => client.packets.push(Buffer.from(raw));
  client.write = (name, data) => client.emit(name, data);
  client.end = reason => { client.reason = reason; client.emit('end'); };
  return client;
}

const tick = () => new Promise(resolve => setImmediate(resolve));
test('multi-slot equipment is rebuilt after destructive decoding for replay and live forwarding', async t => {
  const { transport } = fixture(t);
  const serializer = mc.createSerializer({ state: 'play', isServer: true, version: '1.21.4' });
  const parser = mc.createDeserializer({ state: 'play', isServer: false, version: '1.21.4' });
  const item = { itemCount: 1, itemId: 1, addedComponentCount: 1, removedComponentCount: 0,
    components: [{ type: 'damage', data: 7 }], removeComponents: [] };
  const packet = { entityId: 3, equipments: [{ slot: 0, item }, { slot: 1, item },
    { slot: 2, item: { itemCount: 0 } }, { slot: 5, item }] };
  const original = serializer.createPacketBuffer({ name: 'entity_equipment', params: packet });
  const decoded = parser.parsePacketBuffer(Buffer.from(original));
  assert.equal(decoded.data.params.equipments.length, 4);
  // Reproduce the old bug: the emitted raw buffer now terminates after slot zero.
  assert(parser.parsePacketBuffer(Buffer.from(decoded.buffer)).metadata.size < original.length);
  transport.capture(decoded.data.params, { state: 'play', name: 'entity_equipment' }, decoded.buffer);
  const local = player();
  transport.attach(local);
  await replayed(transport);
  assert.deepEqual(local.packets.at(-1), original);
  const live = parser.parsePacketBuffer(Buffer.from(original));
  transport.capture(live.data.params, { state: 'play', name: 'entity_equipment' }, live.buffer);
  assert.deepEqual(local.packets.at(-1), original);
  const received = parser.parsePacketBuffer(Buffer.from(local.packets.at(-1)));
  assert.equal(received.metadata.size, original.length);
  assert.deepEqual(received.data.params, packet);
});

test('signed chat precedes dependent messages in late-join history', async t => {
  const { transport, capture } = fixture(t);
  capture('player_chat', Buffer.from('signed-chat'));
  capture('hide_message', Buffer.from('signature-reference'));
  capture('system_chat', Buffer.from('transient-chat'));
  const local = player();
  transport.attach(local);
  await replayed(transport);
  assert.deepEqual(local.packets.map(raw => raw.toString()), ['login', 'signed-chat', 'signature-reference']);
});

test('closed local socket stops replay and live writes without losing upstream history', async t => {
  const { transport, capture } = fixture(t);
  const local = player();
  local.socket = { destroyed: false };
  transport.attach(local);
  local.socket.destroyed = true;
  await tick();
  assert.equal(transport.replay, null);
  assert.equal(transport.player, null);
  assert.equal(local.packets.length, 0);
  const second = player();
  transport.attach(second);
  await replayed(transport);
  const count = second.packets.length;
  second.socket = { writableEnded: true };
  capture('block_change', Buffer.from('after-close'));
  assert.equal(second.packets.length, count);
  assert.equal(transport.player, null);
  assert.equal(transport.unavailable, null);
});

async function replayed(transport) {
  for (let i = 0; i < 1000 && transport.replay; i++) await tick();
  assert.equal(transport.replay, null);
}

test('history exceeding old 64 MiB limit remains complete without retained packet arrays', async t => {
  const { transport, capture } = fixture(t);
  const packet = Buffer.alloc(1024 * 1024, 7);
  for (let i = 0; i < 65; i++) capture('map_chunk', packet);
  assert(transport.bytes > 64 * 1024 * 1024);
  assert.equal(transport.unavailable, null);
  assert.equal(transport.history, undefined);
  const local = player();
  let count = 0;
  local.writeRaw = raw => {
    if (count++ === 0) assert.equal(raw.toString(), 'login');
    else assert.deepEqual(raw, packet);
  };
  transport.attach(local);
  await replayed(transport);
  assert.equal(count, 66);
});

test('replay observes backpressure and captures concurrent updates exactly once in order', async t => {
  const { transport, capture } = fixture(t);
  capture('map_chunk', Buffer.from('chunk'));
  const local = player();
  local.framer.writableNeedDrain = true;
  let position;
  local.on('position', data => { position = data; });
  let forwarded = 0;
  transport.writeRaw = () => forwarded++;
  transport.attach(local);
  await tick();
  assert.equal(local.packets.length, 0);
  local.emit('packet', {}, { state: 'play', name: 'use_item' }, Buffer.from('input'));
  assert.equal(forwarded, 0);
  capture('block_change', Buffer.from('change'));
  capture('update_time', Buffer.from('time'));
  local.framer.writableNeedDrain = false;
  local.framer.emit('drain');
  await replayed(transport);
  assert.deepEqual(local.packets.map(raw => raw.toString()), ['login', 'chunk', 'change', 'time']);
  assert.equal(position.x, 1);
  capture('block_change', Buffer.from('live'));
  assert.equal(local.packets.at(-1).toString(), 'live');
  local.emit('packet', {}, { state: 'play', name: 'use_item' }, Buffer.from('input'));
  assert.equal(forwarded, 1);
});

test('backpressure after a write stops replay until drain', async t => {
  const { transport, capture } = fixture(t);
  capture('map_chunk', Buffer.from('chunk'));
  const local = player();
  const write = local.writeRaw;
  local.writeRaw = raw => { write(raw); local.framer.writableNeedDrain = true; };
  transport.attach(local);
  await tick();
  await tick();
  assert.equal(local.packets.length, 1);
  local.writeRaw = write;
  local.framer.writableNeedDrain = false;
  local.framer.emit('drain');
  await replayed(transport);
  assert.deepEqual(local.packets.map(raw => raw.toString()), ['login', 'chunk']);
});

test('detach cancels replay and drain listener; reconnect replays from the start', async t => {
  const { transport } = fixture(t);
  const first = player();
  first.framer.writableNeedDrain = true;
  transport.attach(first);
  await tick();
  assert.equal(first.framer.listenerCount('drain'), 1);
  first.end();
  assert.equal(first.framer.listenerCount('drain'), 0);
  assert.equal(transport.bot.physicsEnabled, true);
  const second = player();
  transport.attach(second);
  await replayed(transport);
  assert.deepEqual(second.packets.map(raw => raw.toString()), ['login']);
});

test('login and reconfiguration invalidate replay; disposal deletes private spool', async t => {
  const { transport, capture } = fixture(t);
  const directory = transport.historyDirectory;
  assert.equal(fs.statSync(directory).mode & 0o777, 0o700);
  const local = player();
  transport.attach(local);
  capture('login', Buffer.from('new-login'));
  assert.match(local.reason, /changed/);
  assert.equal(fs.existsSync(directory), false);
  assert.equal(transport.position, null);
  const nextDirectory = transport.historyDirectory;
  capture('start_configuration', Buffer.from('configure'));
  assert.equal(fs.existsSync(nextDirectory), false);
  assert.deepEqual(transport.registries, {});
  transport.dispose();
  transport.dispose();
  capture('login', Buffer.from('ignored'));
  assert.equal(transport.historyDirectory, null);
});

test('storage failure preserves upstream/live forwarding and blocks incomplete replay', async t => {
  const { transport, capture } = fixture(t);
  const local = player();
  transport.attach(local);
  await replayed(transport);
  capture('map_chunk', Buffer.alloc(0));
  assert.match(transport.unavailable, /storage failed/);
  assert.equal(transport.client.state, 'play');
  assert.equal(transport.player, local);
  assert.equal(transport.historyFd, undefined);
  capture('login', Buffer.from('fresh-login'));
  capture('position', Buffer.from('position'));
  assert.equal(transport.unavailable, null);
});

test('truncated journal fails safely instead of replaying partial state', async t => {
  const { transport } = fixture(t);
  fs.ftruncateSync(transport.historyFd, 4);
  const local = player();
  transport.attach(local);
  await replayed(transport);
  assert.match(local.reason, /Unable to replay/);
  assert.equal(transport.player, null);
  assert.equal(transport.bot.physicsEnabled, true);
});

test('replay timeout releases player and drain listener without dropping upstream', async t => {
  const { transport } = fixture(t);
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const local = player();
  local.framer.writableNeedDrain = true;
  transport.attach(local);
  await tick();
  t.mock.timers.tick(120000);
  assert.match(local.reason, /timed out/);
  assert.equal(transport.player, null);
  assert.equal(local.framer.listenerCount('drain'), 0);
  assert.equal(transport.unavailable, null);
});

test('disk write failure ends pending replay, cleans storage, and permits fresh login', async t => {
  const { transport, capture } = fixture(t);
  const directory = transport.historyDirectory;
  const local = player();
  local.framer.writableNeedDrain = true;
  transport.attach(local);
  await tick();
  const write = t.mock.method(fs, 'writeSync', () => { throw new Error('ENOSPC'); });
  capture('map_chunk', Buffer.from('chunk'));
  assert.match(local.reason, /store/);
  assert.equal(fs.existsSync(directory), false);
  assert.match(transport.unavailable, /storage failed/);
  write.mock.restore();
  capture('login', Buffer.from('fresh-login'));
  capture('position', Buffer.from('position'));
  assert.equal(transport.unavailable, null);
});

test('configuration payload survives plugin mutation and serializes on repeated 1.21.4 joins', t => {
  const { transport } = fixture(t);
  const original = Buffer.from([7, ...Buffer.from('vanilla')]);
  const packet = { channel: 'minecraft:brand', data: Buffer.from(original) };
  transport.capture(packet, { state: 'configuration', name: 'custom_payload' }, Buffer.alloc(0));
  packet.data.fill(0);
  packet.data = 'vanilla'; // minecraft-protocol channel parsers replace this field.
  const saved = transport.configuration.get('minecraft:brand');
  assert.notEqual(saved.data, packet);
  assert(Buffer.isBuffer(saved.data.data));
  assert.deepEqual(saved.data.data, original);
  const serializer = mc.createSerializer({ state: 'configuration', isServer: true, version: '1.21.4' });
  const parser = mc.createDeserializer({ state: 'configuration', isServer: false, version: '1.21.4' });
  for (let join = 0; join < 2; join++) {
    const raw = serializer.createPacketBuffer({ name: saved.name, params: saved.data });
    const decoded = parser.parsePacketBuffer(raw).data;
    assert.equal(decoded.name, 'custom_payload');
    assert.equal(decoded.params.channel, 'minecraft:brand');
    assert.deepEqual(decoded.params.data, original);
  }
});

test('control reset precedes handoff; configuration retains only active resource packs', async t => {
  const { transport } = fixture(t);
  transport.bot.clearControlStates = () => assert.equal(transport.player, undefined);
  const configuration = (name, data) => transport.capture(data, { state: 'configuration', name }, Buffer.alloc(0));
  configuration('add_resource_pack', { uuid: 'a', url: 'https://example.com/a' });
  configuration('add_resource_pack', { uuid: 'b', url: 'https://example.com/b' });
  configuration('remove_resource_pack', { uuid: 'a' });
  assert.deepEqual([...transport.configuration.keys()], ['resource_pack:b']);
  configuration('remove_resource_pack', {});
  assert.equal(transport.configuration.size, 0);
  const local = player();
  transport.attach(local);
  await replayed(transport);
  transport.writeRaw = () => assert.fail('Local resource pack response leaked upstream');
  local.emit('packet', {}, { state: 'play', name: 'resource_pack_receive' }, Buffer.from('response'));
});
