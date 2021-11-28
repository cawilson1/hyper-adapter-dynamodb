import { marshall, R } from "../deps.js";
import { _throw } from "./utils.js";
import { TableName } from "./constants.js";
import {
  queryAll,
  getByKeys,
  startKeyFn,
  endKeyFn,
  startAndEndKeyFn,
  limitFn,
  sortDesc,
} from "./listDocHelpers.js";

const { isEmpty, prop, map, merge } = R;

// create meta doc (for first time) - table will already be created
// pk = name, sk = 'meta', body = {indexes: [], active: true/false, dateTimeCreated: date}
// initial setup, maybe count as well
export const createDatabase = (ddb) => async (name) => {
  return await ddb.putItem({
    TableName,
    Item: marshall({
      indexes: [],
      active: true,
      dateTimeCreated: new Date().toISOString(),
      pk: name,
      sk: "meta",
    }),
  });
};

// meta doc: updates active to false and removes all docs that match pk except meta(?) or deletes entire doc?
//eventually batch get all items with this pk and then batch delete them
export const removeDatabase = (ddb) => (name) => {
  return ddb.putItem({
    TableName,
    Item: marshall({
      active: false,
      pk: name,
      sk: "meta",
    }),
  });
};

// put item into table/meta doc
// pk = db, sk = id, body = doc
export const createDocument =
  (ddb) =>
  async ({ db, id, doc }) => {
    if (isEmpty(doc)) return _throw("empty doc");
    const successful = await ddb.putItem({
      TableName,
      Item: marshall({ ...doc, pk: db, sk: id }),
      ConditionExpression: "attribute_not_exists(#s)",
      ExpressionAttributeNames: { "#s": "sk" },
    });
    if (successful) return id;
  };

export const retrieveDocument =
  (ddb) =>
  async ({ db, id }) => {
    const response = await ddb.getItem({
      TableName,
      Key: marshall({ pk: db, sk: id }),
    });
    if (isEmpty(response)) _throw("no doc");
    return response;
  };

// should this be implemented more like a put
export const updateDocument =
  (ddb) =>
  async ({ db, id, doc }) => {
    const successful = await ddb.putItem({
      TableName,
      Item: marshall({ ...doc, pk: db, sk: id }),
    });

    if (successful) return id;
    //polite patch request below
    // const { updateExp, expAttNames, expAttVals } = updateExpBuilder(doc);

    // const successful = await ddb.updateItem({
    //   TableName,
    //   Key: marshall({ pk: db, sk: id }),
    //   UpdateExpression: updateExp,
    //   ExpressionAttributeNames: expAttNames,
    //   ExpressionAttributeValues: expAttVals,
    //   ReturnValues: "ALL_OLD",
    // });

    // if (successful) return id;
  };

export const removeDocument =
  (ddb) =>
  async ({ db, id }) => {
    const successful = await ddb.deleteItem({
      TableName,
      Key: marshall({ pk: db, sk: id }),
      ReturnValues: "ALL_OLD",
    });
    if (successful) return successful;
  };

export const queryDocuments = (ddb) => async (query) => {
  const params = {
    Statements: [
      {
        Statement: `SELECT * FROM "${TableName}" WHERE pk='firsttable' AND sk='2'`,
      },
    ],
  };
  const result = await ddb.batchExecuteStatement(params);
  console.log(result);
  return result;
};

//Query has 1 mb limit
//should multiple types of conditions be composed?, ex endkey=1004&descending=true
export const listDocuments =
  (ddb) =>
  async ({ db, limit, startkey, endkey, keys, descending }) => {
    if (!limit && !startkey && !endkey && !keys && !descending)
      return queryAll({ ddb, db });
    else if (keys) return getByKeys({ ddb, db, keys });
    else if (startkey && endkey)
      return startAndEndKeyFn({ ddb, db, startkey, endkey });
    else if (startkey) return startKeyFn({ ddb, db, startkey });
    else if (endkey) return endKeyFn({ ddb, db, endkey });
    else if (limit) return limitFn({ ddb, db, limit });
    else if (descending) return sortDesc({ ddb, db });
  };

const requestType = (db) => (doc) =>
  doc._deleted
    ? {
        DeleteRequest: {
          Key: marshall({ pk: db, sk: doc.id }),
        },
      }
    : {
        PutRequest: {
          Item: marshall({ ...doc, pk: db, sk: doc.id }),
        },
      };

//do this 25 at a time (ddb limit)
export const bulkDocuments =
  (ddb) =>
  async ({ db, docs }) => {
    const ids = map(prop("id"))(docs); //success status per id req'd in the response
    const params = {
      RequestItems: {
        [TableName]: docs.map(requestType(db)),
      },
    };
    return ddb.batchWriteItem(params).then(merge({ ids }));
  };
