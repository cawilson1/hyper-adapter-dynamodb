// deno-lint-ignore-file no-unused-vars

import { crocks } from "./deps.js";
import * as lib from "./lib/dynamodb.js";
import {
  notOk,
  okGetDoc,
  okDocs,
  okId,
  ok,
  okQuery,
  notOkCreateDoc,
  notOkGetDoc,
  okDeleteDoc,
  okListDocs,
  okBulkDocs
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
    bulkDocuments: Async.fromPromise(lib.bulkDocuments(ddb)),
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
   * @returns {Promise<Response>}
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
      .bimap(notOk, okListDocs)
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
    console.log(client)
    // could be put or delete
    // pk = db, sk = id from each docs (may need error check that docs have an id)
    return client
      .bulkDocuments({ db, docs })
      .bimap(notOk, okBulkDocs)
      .toPromise();
  }

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
