// CommonJS entry point for @ironfighter23/aether-core.
//
// require('@ironfighter23/aether-core') returns the Aether class
// directly, matching the historical UMD shape of aether.js so
// downstream code that already does
//
//     const Aether = require('@ironfighter23/aether-core');
//
// keeps working with no migration.

'use strict';

const Aether = require('./aether.cjs');

module.exports = Aether;
module.exports.Aether = Aether;
module.exports.default = Aether;
