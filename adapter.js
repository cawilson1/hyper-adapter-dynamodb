// deno-lint-ignore-file no-unused-vars

import { crocks } from "./deps.js";
import * as lib from "./lib/dynamodb.js";
import {
  notOk,
  okGetDoc,
  okGetDocs,
  okId,
  ok,
  okQuery,
  notOkCreateDoc,
  notOkGetDoc,
  okDeleteDoc,
} from "./lib/responseBuilders.js";

const { Async } = crocks;

/**
 *
 * @typedef {Object} CreateDocumentArgs
 * @property {string} db
 * @property {string} id
 * @property {object} doc
 *
 * @typedef {Object} RetrieveDocumentArgs
 * @property {string} db
 * @property {string} id
 *
 * @typedef {Object} QueryDocumentsArgs
 * @property {string} db
 * @property {QueryArgs} query
 *
 * @typedef {Object} QueryArgs
 * @property {object} selector
 * @property {string[]} fields
 * @property {number} limit
 * @property {object[]} sort
 * @property {string} use_index
 *
 * @typedef {Object} IndexDocumentArgs
 * @property {string} db
 * @property {string} name
 * @property {string[]} fields
 *
 * @typedef {Object} ListDocumentArgs
 * @property {string} db
 * @property {number} limit
 * @property {string} startkey
 * @property {string} endkey
 * @property {string[]} keys
 *
 * @typedef {Object} BulkDocumentsArgs
 * @property {string} db
 * @property {object[]} docs
 *
 * @typedef {Object} Response
 * @property {boolean} ok
 */

/**
 *
 * @param {{ DynamoDB: any, factory: any }} aws
 * @returns
 */
// NOTE: The `function ...` style here differs from other files in the project
//       that use the `const ...` anonymous (but not) function style. If you use
//       TypeScript, then IMO go the `const ...` route because of how easy it is
//       to handle interfaces, but if you don't, then I'd make it all as
//       consistent as possible.
export default function (ddb) {
  const client = {
    createDatabase: Async.fromPromise(lib.createDatabase(ddb)),
    removeDatabase: Async.fromPromise(lib.removeDatabase(ddb)),
    createDocument: Async.fromPromise(lib.createDocument(ddb)),
    retrieveDocument: Async.fromPromise(lib.retrieveDocument(ddb)),
    updateDocument: Async.fromPromise(lib.updateDocument(ddb)),
    removeDocument: Async.fromPromise(lib.removeDocument(ddb)),
    queryDocuments: Async.fromPromise(lib.queryDocuments(ddb)),
    listDocuments: Async.fromPromise(lib.listDocuments(ddb)),
  };

  /**
   * @param {string} name
   * @returns {Promise<Response>}
   */
  function createDatabase(name) {
    return client.createDatabase(name).bimap(notOk, ok).toPromise();
  }

  /**
   * @param {string} name
   * @returns {Promise<Response>}
   */
  function removeDatabase(name) {
    return client.removeDatabase(name).bimap(notOk, ok).toPromise();
  }

  /**
   * @param {CreateDocumentArgs}
   * @returns {Promise<Response>}
   */
  function createDocument({ db, id, doc }) {
    return client
      .createDocument({ db, id, doc })
      // NOTE: These `bimap` handlers sometimes take the name of the action and
      //       sometimes take the name of the response shape. I think it's worth
      //       trying to give every action its own (notOkAction, okAction)-style
      //       of response handlers. If some are the exact same, then they can
      //       export the same thing at their source (e.g., `okId`). This means
      //       that if you need to make changes, you don't need to make them at
      //       this level but the handler level.
      .bimap(notOkCreateDoc, okId)
      .toPromise();
  }

  /**
   * @param {RetrieveDocumentArgs}
   * @returns {Promise<Response>}
   */
  function retrieveDocument({ db, id }) {
    return client
      .retrieveDocument({ db, id })
      .bimap(notOkGetDoc, okGetDoc)
      .toPromise();
  }

  /**
   * @param {CreateDocumentArgs}
   * @returns {Promise<Response>}
   */
  function updateDocument({ db, id, doc }) {
    return client
      .updateDocument({ db, id, doc })
      .bimap(notOk, okId)
      .toPromise();
  }

  /**
   * @param {RetrieveDocumentArgs}
   * @returns {Promise<Response>}
   */
  async function removeDocument({ db, id }) {
    return client
      .removeDocument({ db, id })
      .bimap(notOk, okDeleteDoc)
      .toPromise();
  }

  /**
   * @param {QueryDocumentsArgs}
   * @returns {Promise<Response>}
   */
  async function queryDocuments({ query }) {
    console.log("query: ", query);
    return client.queryDocuments(query).bimap(notOk, okQuery).toPromise();
  }

  /**
   *
   * @param {IndexDocumentArgs}
   * @returns {Promise<Response>}
   */
  async function indexDocuments({ db, name, fields }) {
    // schema json object containing db, name, fields
    // insert into meta doc each new index
    // tbd
  }

  /**
   *
   * @param {ListDocumentArgs}
   * @returns {Promise<Response>} NOTE: This returns `ok` and `docs`, but
   *                                    `Response` denotes only `ok`.
   */
  async function listDocuments({
    db,
    limit,
    startkey,
    endkey,
    keys,
    descending,
  }) {
    return client
      .listDocuments({
        db,
        limit,
        startkey,
        endkey,
        keys,
        descending,
      })
      .bimap(notOk, okGetDocs)
      .toPromise();
    // pk = db, sk = startkey, endkey, keys
    // use cases:
    // 1. query using only db => sends all docs in db
    // 2. limit => sends back first n docs in db
    // 3. startkey and endkey => sends back all docs in range (inclusive)
    // 4. keys => sends back all docs with the sk matching each key in array of keys
    // descending => responses are returned in descending order; used with any
  }

  /**
   *
   * @param {BulkDocumentsArgs}
   * @returns {Promise<Response>}
   */
  async function bulkDocuments({ db, docs }) {
    // could be put or delete
    // pk = db, sk = id from each docs (may need error check that docs have an id)
  }


// NOTE: Every one of these gets converted from a Promise to an Async and back
//       to a Promise. I'd consider writing a single generic function that
//       composes these actions together for you. You've got `Async.fromPromise`
//       and crocks' `asyncToPromise` that you could leverage somehow so that
//       all your functions lose the `client` and `.toPromise()` and end up
//       being just like:
//
//       listDocumentsAsync({ ... })
//         .bimap(notOkListDocuments, okListDocuments)
//
//       I guess what I'm really getting at is that the individual functions
//       don't control getting turned into an Async, but they control turning
//       themselves back into a Promise, and that difference of control feels
//       weird to me — like they should just be Asyncs, and something else
//       handles this conversion control.

  return Object.freeze({
    createDatabase,
    removeDatabase,
    createDocument,
    retrieveDocument,
    updateDocument,
    removeDocument,
    queryDocuments,
    indexDocuments,
    listDocuments,
    bulkDocuments,
  });
}
