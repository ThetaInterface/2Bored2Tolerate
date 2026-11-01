// Disk-backed wire history: never evict dependencies or synthesize world packets.
const { EventEmitter } = require('events');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const mc = require('minecraft-protocol');
const mineflayer = require('mineflayer');

const TRANSIENT = new Set([
  'keep_alive', 'ping', 'kick_disconnect', 'position', 'update_time',
  'system_chat', 'player_chat', 'profileless_chat', 'sound_effect',
  'named_sound_effect', 'world_particles', 'entity_sound_effect',
]);
// These responses belong to the upstream protocol client, not the local player.
const AUTOMATIC = new Set([
  'keep_alive', 'pong', 'teleport_confirm', 'chunk_batch_received',
  'configuration_acknowledged', 'finish_configuration', 'login_acknowledged',
  'message_acknowledgement', 'chat_session_update',
  'resource_pack_receive',
  'chat_message', 'chat_command', 'chat_command_signed',
]);

class Transport extends EventEmitter {
  constructor(options, client = mc.createClient(options), maxBytes = 256 * 1024) {
    super();
    this.client = client;
    this.maxBytes = maxBytes;
    this.registries = {};
    this.configuration = new Map();
    this.resetHistory();
    // Register before mineflayer so capture does not depend on plugin mutations.
    client.on('packet', (data, meta, raw) => this.capture(data, meta, raw));
    client.once('end', () => this.dispose());
    this.write = client.write.bind(client);
    this.writeRaw = client.writeRaw.bind(client);
    client.write = (name, data) => {
      if (!this.player || client.state !== 'play' || AUTOMATIC.has(name)) {
        this.write(name, data);
      }
    };
    this.bot = mineflayer.createBot({ ...options, client });
    // Chat is re-signed by the authenticated upstream session.
    this.bot.on('error', () => {});
  }

  resetHistory() {
    if (this.replay) {
      const player = this.player;
      this.detach();
      player.end('Upstream changed during history replay. Please reconnect.');
    }
    this.closeHistory();
    this.bytes = 0;
    this.historyError = null;
    this.login = null;
    this.position = null;
    this.time = null;
  }

  closeHistory() {
    if (this.historyFd !== undefined) {
      try { fs.closeSync(this.historyFd); } catch (error) { this.historyError = error; }
      this.historyFd = undefined;
    }
    if (this.historyDirectory) {
      try {
        fs.rmSync(this.historyDirectory, { recursive: true, force: true });
      } catch (error) { this.historyError = error; }
      this.historyDirectory = null;
    }
  }

  dispose() {
    this.disposed = true;
    const player = this.player;
    this.detach();
    player?.end('Upstream disconnected.');
    this.closeHistory();
  }

  appendHistory(raw) {
    if (this.historyError) return;
    try {
      // A corrupt or pathological single packet must not cause an unbounded allocation.
      if (!raw.length || raw.length > 64 * 1024 * 1024) throw new Error('Invalid history packet size');
      if (this.historyFd === undefined) {
        this.historyDirectory = fs.mkdtempSync(path.join(os.tmpdir(), '2b2w-history-'));
        this.historyFd = fs.openSync(path.join(this.historyDirectory, 'packets'), 'wx+', 0o600);
      }
      const header = Buffer.allocUnsafe(4);
      header.writeUInt32BE(raw.length);
      let offset = this.bytes;
      // Synchronous writes bound the capture queue even if the disk is slow.
      for (const buffer of [header, raw]) {
        let written = 0;
        while (written < buffer.length) {
          const count = fs.writeSync(this.historyFd, buffer, written, buffer.length - written, offset);
          if (!count) throw new Error('Incomplete history write');
          written += count;
          offset += count;
        }
      }
      this.bytes = offset;
    } catch (error) {
      this.historyError = error;
      if (this.replay) this.failReplay('Unable to read or store upstream history.');
      this.closeHistory();
    }
  }

  capture(data, meta, raw) {
    if (this.disposed) return;
    if (meta.state === 'configuration') {
      if (meta.name === 'registry_data') this.registries[data.id] = data;
      if (meta.name === 'add_resource_pack') {
        this.configuration.set(`resource_pack:${data.uuid}`, { name: meta.name, data: structuredClone(data) });
      }
      if (meta.name === 'remove_resource_pack') {
        for (const key of this.configuration.keys()) {
          if (key.startsWith('resource_pack:') && (data.uuid == null || key === `resource_pack:${data.uuid}`)) {
            this.configuration.delete(key);
          }
        }
      }
      if (['tags', 'enabled_features', 'custom_payload'].includes(meta.name)) {
        this.configuration.set(meta.name === 'custom_payload' ? data.channel : meta.name,
          { name: meta.name, data });
      }
      return;
    }
    if (meta.state !== 'play') return;
    if (meta.name === 'start_configuration') {
      // Registry indices can change. Never replay an old world with new registries.
      this.player?.end('Server is reconfiguring. Rejoin once the transfer completes.');
      this.detach();
      this.registries = {};
      this.configuration.clear();
      this.resetHistory();
      return;
    }
    if (meta.name === 'login') {
      this.resetHistory();
      this.login = structuredClone(data);
    }
    if (meta.name === 'position') this.position = structuredClone(data);
    if (meta.name === 'update_time') this.time = Buffer.from(raw);
    if (!TRANSIENT.has(meta.name)) this.appendHistory(raw);
    if (this.player && !this.replay && !['keep_alive', 'ping'].includes(meta.name)) {
      this.player.writeRaw(raw);
      const stream = this.player.compressor || this.player.framer;
      if (stream.writableLength > 8 * 1024 * 1024) {
        const player = this.player;
        this.detach();
        player.end('Local connection cannot keep up with upstream. Please reconnect.');
      }
    }
  }

  get unavailable() {
    if (this.disposed) return 'Upstream is disconnected.';
    if (this.historyError) return 'Late-join history storage failed. Existing upstream connection is preserved.';
    if (!this.login || !this.position || !Object.keys(this.registries).length || this.client.state !== 'play') {
      return 'Upstream login is not ready yet. Please reconnect shortly.';
    }
    return null;
  }

  attach(player) {
    if (this.unavailable) throw new Error(this.unavailable);
    if (this.player) throw new Error('A player is already connected.');
    const entity = this.bot.entity;
    const position = entity?.position;
    if (!position) throw new Error('Upstream player position is not ready.');
    this.bot.clearControlStates();
    this.player = player;
    this.bot.physicsEnabled = false;
    player.on('packet', (data, meta, raw) => {
      if (this.player !== player || this.replay || meta.state !== 'play' || this.client.state !== 'play') return;
      if (['chat_message', 'chat_command', 'chat_command_signed'].includes(meta.name)) {
        // minecraft-protocol owns upstream chat signing and acknowledgement state.
        this.client.chat(meta.name === 'chat_message' ? data.message : `/${data.command}`);
        return;
      }
      if (AUTOMATIC.has(meta.name)) return;
      if (['position', 'position_look', 'look', 'flying'].includes(meta.name)) {
        if (data.x !== undefined) entity.position.set(data.x, data.y, data.z);
        if (data.yaw !== undefined) {
          entity.yaw = (180 - data.yaw) * Math.PI / 180;
          entity.pitch = -data.pitch * Math.PI / 180;
        }
        entity.onGround = data.flags?.onGround ?? data.onGround;
      }
      this.writeRaw(raw);
    });
    player.once('end', () => { if (this.player === player) this.detach(); });
    this.replay = { offset: 0, stream: player.compressor || player.framer };
    this.replay.timeout = setTimeout(() => this.failReplay('History replay timed out. Please reconnect.'), 120000);
    this.replay.timeout.unref();
    this.replay.immediate = setImmediate(() => this.pumpReplay());
  }

  failReplay(reason) {
    const player = this.player;
    this.detach();
    player?.end(reason);
  }

  pumpReplay() {
    const replay = this.replay;
    if (!replay) return;
    const player = this.player;
    try {
      if (!replay.stream.writable || player.state !== 'play') throw new Error('Local connection closed');
      const waitForDrain = () => {
        replay.onDrain = () => {
          replay.onDrain = null;
          replay.immediate = setImmediate(() => this.pumpReplay());
        };
        replay.stream.once('drain', replay.onDrain);
      };
      if (replay.stream.writableNeedDrain) return waitForDrain();
      let sent = 0;
      const header = Buffer.allocUnsafe(4);
      const read = (buffer, offset) => {
        let size = 0;
        while (size < buffer.length) {
          const count = fs.readSync(this.historyFd, buffer, size, buffer.length - size, offset + size);
          if (!count) throw new Error('Incomplete history record');
          size += count;
        }
      };
      while (replay.offset < this.bytes && sent < Math.max(1, this.maxBytes)) {
        read(header, replay.offset);
        const length = header.readUInt32BE();
        if (!length || length > 64 * 1024 * 1024 || replay.offset + 4 + length > this.bytes) {
          throw new Error('Invalid history record');
        }
        const raw = Buffer.allocUnsafe(length);
        read(raw, replay.offset + 4);
        replay.offset += 4 + length;
        sent += 4 + length;
        player.writeRaw(raw);
        if (replay.stream.writableNeedDrain) return waitForDrain();
      }
      if (replay.offset < this.bytes) {
        replay.immediate = setImmediate(() => this.pumpReplay());
        return;
      }
      // No await between reaching the journal tail and enabling live forwarding.
      // Time/position are snapshots; historical relative teleports must not replay.
      if (this.time) player.writeRaw(this.time);
      const entity = this.bot.entity;
      const position = entity.position;
      player.write('position', {
        teleportId: -1, x: position.x, y: position.y, z: position.z,
        dx: 0, dy: 0, dz: 0,
        yaw: 180 - entity.yaw * 180 / Math.PI,
        pitch: -entity.pitch * 180 / Math.PI, flags: 0,
      });
      clearTimeout(replay.timeout);
      this.replay = null;
    } catch {
      this.failReplay('Unable to replay upstream history. Please reconnect.');
    }
  }

  detach() {
    if (this.replay) {
      clearTimeout(this.replay.timeout);
      clearImmediate(this.replay.immediate);
      if (this.replay.onDrain) this.replay.stream.removeListener('drain', this.replay.onDrain);
      this.replay = null;
    }
    this.player = null;
    if (this.bot) this.bot.physicsEnabled = true;
  }
}

module.exports = Transport;
