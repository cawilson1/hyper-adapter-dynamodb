import { marshall, R } from "../deps.js";
import { _throw } from "./utils.js";
import { TableName, EMPTYDOC } from "./constants.js";
import {
  queryAll,
  getByKeys,
  startKeyFn,
  endKeyFn,
  startAndEndKeyFn,
  limitFn,
  sortDesc,
} from "./listDocHelpers.js";
import { queryBuilder } from "./queryUtils/partiqlQueryBuilder.js";
import { doBulkUpdate } from "./bulkHelpers.js";
import { retrieveIndexFields, queryUtils } from "./queryUtils/queryUtils.js";

const { always, isEmpty, map } = R;

// create meta doc
export const createDatabase = (ddb) => (name) =>
  ddb.putItem({
    TableName,
    Item: marshall({
      indexes: [],
      active: true,
      dateTimeCreated: new Date().toISOString(),
      pk: name,
      sk: "meta",
    }),
  });

//deletes require pk and sk. first step is get all sk vals via queryAll
export const removeDatabase = (ddb) => (name) =>
  queryAll({ ddb, db: name }).then((response) =>
    ddb.batchWriteItem({
      RequestItems: {
        [TableName]: map((item) => ({
          DeleteRequest: { Key: { pk: item.pk, sk: item.sk } },
        }))(response),
      },
    })
  );

export const createDocument =
  (ddb) =>
  async (
    { db, id, doc } //left async bc _throw is not a promise
  ) =>
    isEmpty(doc)
      ? _throw(EMPTYDOC)
      : ddb
          .putItem({
            TableName,
            Item: marshall({ ...doc, pk: db, sk: id }),
            ConditionExpression: "attribute_not_exists(#s)",
            ExpressionAttributeNames: { "#s": "sk" },
          })
          .then(always(id));

// NOTE: This should always return some sort of Promise with a value, right?
//       What happens if it's not successful? Should this throw in this
//       case, too? This applies to other functions in here, too.
//@rpearce - for these cases, the item we care about is the id for the hyper api response. The fn is async so anything it returns is a promise
//        but we only want to send it back if the putItem was successful (not relevant, but all successes for putItem are an empty obj)
//        If the operation was not successful putItem throws an error describing why.
//        Basically, this if(successful) is only there to wait and see that we get some valid response.
//        If there is not a valid response ddb.putItem throws an error which is passed downstream into notOk via bimap.
//        These seems like a simple solution that checks the boxes needed, but open to suggestions to improve this

//        I updated the actual function ^ to look more "promisy".
//        They're essentially doing the same, but version below is arguably a trojan horse, hiding the "promisiness", and maybe deceptive in its purpose

//     old logic:
// if (isEmpty(doc)) {
//   return _throw("empty doc");
// }

// const successful = await ddb.putItem({
//   TableName,
//   Item: marshall({ ...doc, pk: db, sk: id }),
//   ConditionExpression: "attribute_not_exists(#s)",
//   ExpressionAttributeNames: { "#s": "sk" },
// });

// if (successful) {
//   return id
// };

export const retrieveDocument =
  (ddb) =>
  async ({ db, id }) => {
    const response = await ddb.getItem({
      TableName,
      Key: marshall({ pk: db, sk: id }),
    });

    if (isEmpty(response)) {
      _throw(EMPTYDOC);
    }

    return response;
  };

export const updateDocument =
  (ddb) =>
  async ({ db, id, doc }) =>
    ddb
      .putItem({
        TableName,
        Item: marshall({ ...doc, pk: db, sk: id }),
      })
      .then(always(id));

export const removeDocument =
  (ddb) =>
  async ({ db, id }) =>
    ddb.deleteItem({
      TableName,
      Key: marshall({ pk: db, sk: id }),
      ReturnValues: "ALL_OLD",
    });

export const queryDocuments =
  (ddb) =>
  async ({ query, db }) => {
    const { selector, fields, limit, sort, use_index } = query;
    const { unmarshallResponse, doSort } = queryUtils({ limit, sort });

    if (isEmpty(selector))
      return queryAll({ ddb, db }).then(unmarshallResponse).then(doSort);
    const params = {
      Statement: queryBuilder({
        selector,
        fields,
        pk: db,
        index: await retrieveIndexFields({ db, ddb, use_index }),
      }),
    };

    return ddb.executeStatement(params).then(unmarshallResponse).then(doSort);
  };

export const indexDocuments =
  (ddb) =>
  ({ db, name, fields }) =>
    ddb.updateItem({
      TableName,
      Key: marshall({ pk: db, sk: "meta" }),
      UpdateExpression: "set #indexes = list_append(#indexes,:fields)",
      ExpressionAttributeNames: {
        "#indexes": "indexes",
      },
      ExpressionAttributeValues: marshall({ ":fields": [{ [name]: fields }] }),
      ReturnValues: "ALL_OLD",
    });

//Query has 1 mb limit
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

export const bulkDocuments =
  (ddb) =>
  ({ db, docs }) =>
    doBulkUpdate({ ddb, db, docs });
