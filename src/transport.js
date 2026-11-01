// A bounded wire-history snapshot: never synthesize version-specific world packets.
const { EventEmitter } = require('events');
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
  'chat_message', 'chat_command', 'chat_command_signed',
]);

class Transport extends EventEmitter {
  constructor(options, client = mc.createClient(options), maxBytes = 64 * 1024 * 1024) {
    super();
    this.client = client;
    this.maxBytes = maxBytes;
    this.registries = {};
    this.configuration = new Map();
    this.resetHistory();
    // Register before mineflayer so capture does not depend on plugin mutations.
    client.on('packet', (data, meta, raw) => this.capture(data, meta, raw));
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
    this.history = [];
    this.bytes = 0;
    this.overflow = false;
    this.login = null;
    this.position = null;
    this.time = null;
  }

  capture(data, meta, raw) {
    if (meta.state === 'configuration') {
      if (meta.name === 'registry_data') this.registries[data.id] = data;
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
    if (!TRANSIENT.has(meta.name) && !this.overflow) {
      this.bytes += raw.length;
      if (this.bytes > this.maxBytes) {
        this.overflow = true;
        this.history = [];
      } else {
        this.history.push(Buffer.from(raw));
      }
    }
    if (this.player && !['keep_alive', 'ping'].includes(meta.name)) {
      this.player.writeRaw(raw);
    }
  }

  get unavailable() {
    if (this.overflow) return 'Late-join history limit reached. Existing upstream connection is preserved.';
    if (!this.login || !this.position || !Object.keys(this.registries).length || this.client.state !== 'play') {
      return 'Upstream login is not ready yet. Please reconnect shortly.';
    }
    return null;
  }

  attach(player) {
    if (this.unavailable) throw new Error(this.unavailable);
    if (this.player) throw new Error('A player is already connected.');
    for (const raw of this.history) player.writeRaw(raw);
    if (this.time) player.writeRaw(this.time);
    const entity = this.bot.entity;
    const position = entity?.position;
    if (!position) throw new Error('Upstream player position is not ready.');
    // Absolute position, not the last upstream teleport's relative coordinates.
    player.write('position', {
      teleportId: -1, x: position.x, y: position.y, z: position.z,
      dx: 0, dy: 0, dz: 0,
      yaw: 180 - entity.yaw * 180 / Math.PI,
      pitch: -entity.pitch * 180 / Math.PI, flags: 0,
    });
    this.player = player;
    this.bot.physicsEnabled = false;
    this.bot.clearControlStates();
    player.on('packet', (data, meta, raw) => {
      if (this.player !== player || meta.state !== 'play' || this.client.state !== 'play') return;
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
  }

  detach() {
    this.player = null;
    if (this.bot) this.bot.physicsEnabled = true;
  }
}

module.exports = Transport;
