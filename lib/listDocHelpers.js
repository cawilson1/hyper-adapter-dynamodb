import { R, marshall } from "../deps.js";
import { TableName } from "./constants.js";
const { prop } = R;

export const queryAll = (ddb, db) =>
  ddb
    .query({
      TableName,
      ExpressionAttributeValues: {
        ":pk": {
          S: db,
        },
      },
      KeyConditionExpression: "pk = :pk",
    })
    .then(prop("Items"));

export const getByKeys = async ({ ddb, db, keys }) => {
  const keysArray = keys.split(",");
  const primaryKeys = keysArray.map((key) => marshall({ pk: db, sk: key }));

  const result = await ddb.batchGetItem({
    RequestItems: {
      [TableName]: {
        Keys: primaryKeys,
      },
    },
  });
  return result?.Responses?.[TableName];
};

export const startKeyFn = ({ ddb, db, startkey }) =>
  ddb
    .query({
      TableName,
      ExpressionAttributeValues: {
        ":pk": {
          S: db,
        },
        ":sk": {
          S: startkey,
        },
      },
      KeyConditionExpression: "pk = :pk and sk >= :sk",
    })
    .then(prop("Items"));

export const endKeyFn = ({ ddb, db, endkey }) =>
  ddb
    .query({
      TableName,
      ExpressionAttributeValues: {
        ":pk": {
          S: db,
        },
        ":sk": {
          S: endkey,
        },
      },
      KeyConditionExpression: "pk = :pk and sk <= :sk",
    })
    .then(prop("Items"));

export const startAndEndKeyFn = ({ ddb, db, startkey, endkey }) =>
  ddb
    .query({
      TableName,
      ExpressionAttributeValues: {
        ":pk": {
          S: db,
        },
        ":start": {
          S: startkey,
        },
        ":end": {
          S: endkey,
        },
      },
      KeyConditionExpression: "pk = :pk and sk BETWEEN :start AND :end",
    })
    .then(prop("Items"));
