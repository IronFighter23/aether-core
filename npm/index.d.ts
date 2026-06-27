// Re-export everything from the canonical type file. Keeping the
// shapes in one place means we cannot drift between the npm-published
// types and the in-repo `web/aether.d.ts` used by `<script>` consumers.
export * from './aether';
export { default } from './aether';
