import { marshall, R } from "../deps.js";
import { _throw } from "./utils.js";

const {isEmpty} = R

const TableName = "hyper-test";

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
      if(isEmpty(doc)) return _throw("empty doc")
      const successful = await ddb.putItem({
        TableName,
        Item: marshall({ ...doc, pk: db, sk: id }),
        ConditionExpression: "attribute_not_exists(#s)",
        ExpressionAttributeNames: {"#s":"sk"}
      });
      if (successful) return id;
    };

export const retrieveDocument =
  (ddb) =>
    ({ db, id }) => {
      return ddb.getItem({
        TableName,
        Key: marshall({ pk: db, sk: id }),
      });
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

      if (successful) return id;
    };

export const queryDocuments = (ddb) => async (query) => {
  const params = {
    Statements: [
      {
        Statement: `SELECT * FROM "${TableName}" WHERE pk='firsttable' AND sk='2'`,
      }
    ]
  };
  const result = await ddb.batchExecuteStatement(params);
  console.log(result)
  return result
};

export const listDocuments =
  (ddb) =>
    async ({ db, limit, startkey, endkey, keys, descending }) => {
      const keysArray = keys.split(",");

      const primaryKeys = keysArray.map((key) => marshall({ pk: db, sk: key }));

      const result = await ddb.batchGetItem({
        RequestItems: {
          [TableName]: {
            Keys: primaryKeys,
          },
        },
      });
      return result.Responses[TableName];
    };
