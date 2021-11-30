// deno-lint-ignore-file no-unused-vars

import { crocks } from "./deps.js";
import * as lib from "./lib/dynamodb.js";
import {
  okCreateDb,
  notOkCreateDb,
  okDestroyDb,
  notOkDestroyDb,
  okGetDoc,
  notOkUpdateDoc,
  okUpdateDoc,
  okQuery,
  notOkQuery,
  okCreateDoc,
  notOkCreateDoc,
  notOkGetDoc,
  okDeleteDoc,
  notOkDeleteDoc,
  okListDocs,
  notOkListDocs,
  okBulkDocs,
  notOkBulkDocs,
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
const adapter = (ddb) => {
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
    client.createDatabase(name).bimap(notOkCreateDb, okCreateDb).toPromise();

  /**
   * @param {string} name
   * @returns {Promise<Response>}
   */
  const removeDatabase = (name) =>
    client.removeDatabase(name).bimap(notOkDestroyDb, okDestroyDb).toPromise();

  /**
   * @param {CreateDocumentArgs}
   * @returns {Promise<Response>}
   */
  const createDocument = ({ db, id, doc }) =>
    client
      .createDocument({ db, id, doc })
      .bimap(notOkCreateDoc, okCreateDoc)
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
    client
      .updateDocument({ db, id, doc })
      .bimap(notOkUpdateDoc, okUpdateDoc)
      .toPromise();

  /**
   * @param {RetrieveDocumentArgs}
   * @returns {Promise<Response>}
   */
  const removeDocument = ({ db, id }) =>
    client
      .removeDocument({ db, id })
      .bimap(notOkDeleteDoc, okDeleteDoc)
      .toPromise();

  /**
   * @param {QueryDocumentsArgs}
   * @returns {Promise<Response>}
   */
  const queryDocuments = ({ query, db }) =>
    client.queryDocuments({ query, db }).bimap(notOkQuery, okQuery).toPromise();

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
      .bimap(notOkListDocs, okListDocs)
      .toPromise();

  /**
   *
   * @param {BulkDocumentsArgs}
   * @returns {Promise<Response>}
   */

  const bulkDocuments = ({ db, docs }) =>
    client
      .bulkDocuments({ db, docs })
      .bimap(notOkBulkDocs, okBulkDocs)
      .toPromise();

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
  //@rpearce - I'm not sure what this composed fn would look like

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
};

export default adapter;
