// ESM entry point for @ironfighter23/aether-core.
//
// The implementation lives in aether.js (a UMD-style module that
// auto-detects ESM/CJS/browser-global). This thin wrapper re-exports
// the Aether class as the default export AND as a named export so
// both ``import Aether from`` and ``import { Aether } from`` work.

import Aether from './aether.cjs';

export { Aether };
export default Aether;
