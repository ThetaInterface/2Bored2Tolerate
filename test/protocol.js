const assert = require('node:assert/strict');
const { once } = require('node:events');
const { test } = require('node:test');
const net = require('node:net');
const mc = require('minecraft-protocol');
const data = require('minecraft-data')('1.21.4');
const nbt = require('prismarine-nbt');
process.env.LOG_ENABLED = 'false';
const config = require('../src/config');
const Transport = require('../src/transport');
const ProxyManager = require('../src/proxy');

test('1.21.4 local upstream, queue join, forwarding, disconnect and rejoin', { timeout: 20000 }, async (t) => {
  Object.assign(config.proxy, { port: 0, bind: '127.0.0.1', onlineMode: false, whitelist: false });
  config.antiAfk.enabled = false;
  const upstream = mc.createServer({ host: '127.0.0.1', port: 0, version: '1.21.4',
    'online-mode': false, registryCodec: data.loginPacket.dimensionCodec });
  const manager = new ProxyManager();
  const locals = [];
  t.after(() => {
    for (const client of locals) client.end();
    manager.stop();
    for (const client of Object.values(upstream.clients)) client.end('Test complete');
    upstream.close();
  });
  await once(upstream, 'listening');
  let remote;
  const equipment = { entityId: 3, equipments: [0, 1, 2, 5].map(slot => ({ slot,
    item: { itemCount: 1, itemId: data.itemsByName.diamond_sword.id,
      addedComponentCount: 2, removedComponentCount: 0,
      components: [{ type: 'damage', data: 7 },
        { type: 'custom_name', data: { type: 'compound', value: { text: nbt.string('Proxy test sword') } } }],
      removeComponents: [] } })) };
  upstream.on('playerJoin', (client) => {
    remote = client;
    client.write('login', data.loginPacket);
    client.write('position', { teleportId: 10, x: 1, y: 80, z: 2,
      dx: 0, dy: 0, dz: 0, yaw: 0, pitch: 0, flags: 0 });
    client.write('window_items', { windowId: 0, stateId: 2,
      items: Array.from({ length: 46 }, () => ({ itemCount: 0 })), carriedItem: { itemCount: 0 } });
    client.write('entity_equipment', equipment);
    client.write('playerlist_header', { header: nbt.comp({ text: nbt.string('Position in queue: 42') }),
      footer: nbt.comp({ text: nbt.string('Queue') }) });
  });
  const conn = new Transport({ host: '127.0.0.1', port: upstream.socketServer.address().port,
    username: 'TestBot', auth: 'offline', version: '1.21.4' });
  manager.conn = conn;
  manager.client = conn.client;
  manager._setupLocalServer();
  await once(manager.server, 'listening');
  await once(conn.client, 'playerlist_header');
  assert.equal(conn.unavailable, null);
  async function join() {
    const client = mc.createClient({ host: '127.0.0.1', port: manager.server.socketServer.address().port,
      username: 'LocalPlayer', auth: 'offline', version: '1.21.4' });
    locals.push(client);
    const packets = [];
    client.on('packet', (packet, meta) => packets.push({ name: meta.name, packet }));
    await once(client, 'position');
    assert(packets.some(p => p.name === 'registry_data'));
    assert.equal(packets.find(p => p.name === 'login').packet.worldState.dimension, 0);
    assert.equal(packets.find(p => p.name === 'window_items').packet.items.length, 46);
     assert(packets.some(p => p.name === 'playerlist_header'));
    assert.deepEqual(packets.find(p => p.name === 'entity_equipment').packet, equipment);
    return client;
  }
  const first = await join();
  // Local chat acknowledgements must not enter the server plugin's unrelated window.
  const acknowledged = once(conn.player, 'message_acknowledgement');
  first.write('message_acknowledgement', { count: 1 });
  await acknowledged;
  assert.equal(manager.finishedQueue, false);
  assert.equal(manager.state.connected, true);
  const movement = once(remote, 'position');
  first.write('position', { x: 12, y: 80, z: 13, flags: { onGround: true, hasHorizontalCollision: false } });
  assert.equal((await movement)[0].x, 12);
  const disconnected = once(conn.player, 'end');
  first.end();
  await disconnected;
  assert.equal(conn.client.state, 'play');
  assert.equal(conn.player, null);
  assert.equal(manager.state.connected, false);
  const second = await join();
  const equipmentUpdate = once(second, 'entity_equipment');
  remote.write('entity_equipment', equipment);
  assert.deepEqual((await equipmentUpdate)[0], equipment);
  assert.equal(conn.bot.entity.position.x, 12);
  const live = once(second, 'update_health');
  remote.write('update_health', { health: 18, food: 19, foodSaturation: 3 });
  assert.equal((await live)[0].health, 18);
  conn.maxBytes = 1;
  const raw = mc.createSerializer({ state: 'play', isServer: true, version: '1.21.4' })
    .createPacketBuffer({ name: 'update_health', params: { health: 18, food: 19, foodSaturation: 3 } });
  conn.capture({}, { state: 'play', name: 'update_health' }, raw);
  assert.equal(conn.unavailable, null);
  const secondDisconnected = once(conn.player, 'end');
  second.end();
  await secondDisconnected;
  await join();
  // Reproduce the socket failure without writing an Error-valued kick packet.
  const failedPlayer = conn.player;
  const failedEnd = new Promise(resolve => failedPlayer.once('end', resolve));
  let kicks = 0;
  const write = failedPlayer.write.bind(failedPlayer);
  failedPlayer.write = (name, packet) => {
    if (name === 'kick_disconnect') kicks++;
    return write(name, packet);
  };
  failedPlayer.socket.emit('error', Object.assign(new Error('write EPIPE'), { code: 'EPIPE' }));
  failedPlayer.emit('error', new Error('Secondary stream failure'));
  await failedEnd;
  assert.equal(kicks, 0);
  assert.equal(conn.player, null);
  assert.equal(conn.replay, null);
  assert.equal(conn.client.state, 'play');
  assert.equal(manager.state.connected, false);
  await join();
});

test('game port rejects HTTP and malformed prefixes before decoding, still answers Minecraft status', { timeout: 10000 }, async (t) => {
  Object.assign(config.proxy, { port: 0, bind: '127.0.0.1', onlineMode: false });
  const manager = new ProxyManager();
  manager._setupLocalServer();
  t.after(() => manager.stop());
  await once(manager.server, 'listening');
  const port = manager.server.socketServer.address().port;
  let accepted = 0;
  manager.server.on('connection', () => accepted++);
  for (const chunks of [[Buffer.from('G'), Buffer.from('ET / HTTP/1.1\r\nHost: localhost\r\n\r\n')],
    [Buffer.from([0x16, 0x03, 0x01])], [Buffer.from([0xff, 0xff, 0xff, 0xff])]]) {
    const socket = net.connect({ host: '127.0.0.1', port });
    t.after(() => socket.destroy());
    await once(socket, 'connect');
    const closed = once(socket, 'close');
    for (const chunk of chunks) {
      socket.write(chunk);
      await new Promise(resolve => setImmediate(resolve));
    }
    await closed;
  }
  assert.equal(accepted, 0);
  const status = await new Promise((resolve, reject) => mc.ping({ host: '127.0.0.1', port, version: '1.21.4' },
    (error, response) => error ? reject(error) : resolve(response)));
  assert.equal(status.version.protocol, 769);
  assert.equal(accepted, 1);
});

test('1.21.4 chunk, entity and equipment packets serialize without legacy shapes', () => {
  const serializer = mc.createSerializer({ state: 'play', isServer: true, version: '1.21.4' });
  const parser = mc.createDeserializer({ state: 'play', isServer: false, version: '1.21.4' });
  const Chunk = require('prismarine-chunk')('1.21.4');
  const chunk = new Chunk({ minY: -64, worldHeight: 384 });
  const packets = [
    ['map_chunk', { x: 0, z: 0, heightmaps: nbt.comp({}), chunkData: chunk.dump(), blockEntities: [],
      skyLightMask: [], blockLightMask: [], emptySkyLightMask: [], emptyBlockLightMask: [], skyLight: [], blockLight: [] }],
    ['spawn_entity', { entityId: 3, objectUUID: '00000000-0000-0000-0000-000000000003', type: 1,
      x: 0, y: 80, z: 0, pitch: 0, yaw: 0, headPitch: 0, objectData: 0, velocity: { x: 0, y: 0, z: 0 } }],
    ['entity_equipment', { entityId: 3, equipments: [{ slot: 0, item: { itemCount: 0 } }] }],
  ];
  for (const [name, params] of packets) {
    const raw = serializer.createPacketBuffer({ name, params });
    assert.equal(parser.parsePacketBuffer(raw).data.name, name);
  }
});
