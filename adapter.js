// deno-lint-ignore-file no-unused-vars

import { crocks } from "./deps.js";
import * as lib from "./lib/dynamodb.js";
import {
  notOk,
  okGetDoc,
  okId,
  ok,
  okQuery,
  notOkCreateDoc,
  notOkGetDoc,
  okDeleteDoc,
  okListDocs,
  okBulkDocs,
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
    indexDocuments: Async.fromPromise(lib.indexDocuments(ddb)),
    retrieveDocument: Async.fromPromise(lib.retrieveDocument(ddb)),
    updateDocument: Async.fromPromise(lib.updateDocument(ddb)),
    removeDocument: Async.fromPromise(lib.removeDocument(ddb)),
    queryDocuments: Async.fromPromise(lib.queryDocuments(ddb)),
    listDocuments: Async.fromPromise(lib.listDocuments(ddb)),
    bulkDocuments: Async.fromPromise(lib.bulkDocuments(ddb)),
  };

  /**
   * @param {string} name
   * @returns {Promise<Response>}
   */
  const createDatabase = (name) =>
    client.createDatabase(name).bimap(notOk, ok).toPromise();

  /**
   * @param {string} name
   * @returns {Promise<Response>}
   */
  const removeDatabase = (name) =>
    client.removeDatabase(name).bimap(notOk, ok).toPromise();

  /**
   * @param {CreateDocumentArgs}
   * @returns {Promise<Response>}
   */
  // NOTE: These `bimap` handlers sometimes take the name of the action and
  //       sometimes take the name of the response shape. I think it's worth
  //       trying to give every action its own (notOkAction, okAction)-style
  //       of response handlers. If some are the exact same, then they can
  //       export the same thing at their source (e.g., `okId`). This means
  //       that if you need to make changes, you don't need to make them at
  //       this level but the handler level.
  const createDocument = ({ db, id, doc }) =>
    client
      .createDocument({ db, id, doc })

      .bimap(notOkCreateDoc, okId)
      .toPromise();

  /**
   * @param {RetrieveDocumentArgs}
   * @returns {Promise<Response>}
   */
  const retrieveDocument = ({ db, id }) =>
    client
      .retrieveDocument({ db, id })
      .bimap(notOkGetDoc, okGetDoc)
      .toPromise();

  /**
   * @param {CreateDocumentArgs}
   * @returns {Promise<Response>}
   */
  const updateDocument = ({ db, id, doc }) =>
    client.updateDocument({ db, id, doc }).bimap(notOk, okId).toPromise();

  /**
   * @param {RetrieveDocumentArgs}
   * @returns {Promise<Response>}
   */
  const removeDocument = ({ db, id }) =>
    client.removeDocument({ db, id }).bimap(notOk, okDeleteDoc).toPromise();

  /**
   * @param {QueryDocumentsArgs}
   * @returns {Promise<Response>}
   */
  const queryDocuments = ({ query, db }) =>
    client.queryDocuments({ query, db }).bimap(notOk, okQuery).toPromise();

  /**
   *
   * @param {IndexDocumentArgs}
   * @returns {Promise<Response>}
   */

  // schema json object containing db, name, fields
  // insert into meta doc each new index
  // tbd
  const indexDocuments = ({ db, name, fields }) =>
    client.indexDocuments({ db, name, fields }).bimap(notOk, ok).toPromise();

  /**
   *
   * @param {ListDocumentArgs}
   * @returns {Promise<Response>} NOTE: This returns `ok` and `docs`, but
   *                                    `Response` denotes only `ok`.
   */
  const listDocuments = ({ db, limit, startkey, endkey, keys, descending }) =>
    client
      .listDocuments({
        db,
        limit,
        startkey,
        endkey,
        keys,
        descending,
      })
      .bimap(notOk, okListDocs)
      .toPromise();

  /**
   *
   * @param {BulkDocumentsArgs}
   * @returns {Promise<Response>}
   */


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
  const bulkDocuments = ({ db, docs }) =>
    client.bulkDocuments({ db, docs }).bimap(notOk, okBulkDocs).toPromise();


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
