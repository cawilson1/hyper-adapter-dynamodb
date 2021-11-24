import { marshall, R } from "../deps.js";
import { _throw } from "./utils.js";

const { isEmpty } = R;

const TableName = "hyper-test";

// create meta doc (for first time) - table will already be created
// pk = name, sk = 'meta', body = {indexes: [], active: true/false, dateTimeCreated: date}
// initial setup, maybe count as well
// NOTE: Can this return ddb.putItem without async/await?
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
// NOTE: I'd put these arguments on the top line, for they don't go over 80
//       characters wide.
export const createDocument =
  (ddb) =>
  async ({ db, id, doc }) => {
    if (isEmpty(doc)) {
      // NOTE: These strings getting thrown should probably be stored as constant
      //       variables to prevent typos when catching and checking errors.
      return _throw("empty doc");
    }

    const successful = await ddb.putItem({
      TableName,
      Item: marshall({ ...doc, pk: db, sk: id }),
      ConditionExpression: "attribute_not_exists(#s)",
      ExpressionAttributeNames: { "#s": "sk" },
    });

    if (successful) {
      return id
    };
    // NOTE: This should always return some sort of Promise with a value, right?
    //       What happens if it's not successful? Should this throw in this
    //       case, too? This applies to other functions in here, too.
  };

export const retrieveDocument =
  (ddb) =>
  async ({ db, id }) => {
    const response = await ddb.getItem({
      TableName,
      Key: marshall({ pk: db, sk: id }),
    });

    if (isEmpty(response)) {
      _throw("no doc")
    };

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

    // NOTE: My personal preference is to bracket and newline all `if`s to
    //       expressly call them out. There's even an eslint option for this :)
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
console.log("DOC BEFORE PROCESS",successful)
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

export const listDocuments =
  (ddb) =>
  async ({ db, limit, startkey, endkey, keys, descending }) => {
    const keysArray = keys.split(",");

    const primaryKeys = keysArray.map((key) => marshall({ pk: db, sk: key }));

    // NOTE: you can probably get rid of `keysArray` and just do
    //       const primaryKeys = keys.split(",").map(...)

    const result = await ddb.batchGetItem({
      RequestItems: {
        [TableName]: {
          Keys: primaryKeys,
        },
      },
    });
    return result.Responses[TableName];
  };
