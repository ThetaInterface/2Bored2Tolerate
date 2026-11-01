const assert = require('node:assert/strict');
const { test } = require('node:test');
const AntiAFK = require('../src/antiafk');

test('return walk faces origin and stop cancels short actions across restart', t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const controls = {};
  let yaw;
  const bot = { entity: { position: { x: 0, y: 80, z: -3 } },
    look(value) { yaw = value; }, setControlState(name, value) { controls[name] = value; } };
  const afk = new AntiAFK(bot, { walk: false, look: false, jump: false, swing: false, sneak: false });
  afk.start();
  afk.origin = { x: 0, y: 80, z: 0 };
  afk._returnToOrigin();
  assert.equal(Math.abs(yaw), Math.PI);
  assert.equal(controls.forward, true);
  assert.equal(afk.actionTimers.size, 1);
  afk.stop();
  assert.equal(afk.actionTimers.size, 0);
  assert.equal(controls.forward, false);
  afk._returnToOrigin();
  assert.equal(controls.forward, false);
  afk.start();
  controls.forward = true;
  t.mock.timers.tick(1000);
  assert.equal(controls.forward, true);
  afk.stop();
});
