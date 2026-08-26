import { readFileSync } from 'node:fs';

const contract = JSON.parse(
  readFileSync(new URL('../../openapi/fleetd-v1.json', import.meta.url), 'utf8'),
);

// Fetch cannot perform a WebSocket upgrade. Keep the operation in the public
// contract, but do not generate an HTTP function that would silently behave
// incorrectly. Consumers still receive the generated Message frame type.
for (const pathItem of Object.values(contract.paths)) {
  for (const [method, operation] of Object.entries(pathItem)) {
    if (operation?.['x-fleetd-websocket']) {
      delete pathItem[method];
    }
  }
}

/** @type {import('@hey-api/openapi-ts').UserConfig} */
export default {
  input: contract,
  output: {
    path: 'src/generated',
    clean: true,
    // This package resolves modules as NodeNext, which requires an explicit
    // extension on every relative import. The generator infers that from the
    // tsconfig it happens to find, so state it here instead: the output must
    // not depend on which directory the generator was run from.
    importFileExtension: '.js',
  },
};
