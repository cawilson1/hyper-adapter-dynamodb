import { marshall, R, unmarshall } from "../deps.js";
import { TableName } from "./constants.js";
const { prop, map, merge } = R;


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

const oneBatch = ({ ddb, db, docs }) => {
  const ids = map(prop("id"))(docs); //success status per id req'd in the response
  const params = {
    RequestItems: {
      [TableName]: docs.map(requestType(db)),
    },
  };
  return ddb.batchWriteItem(params).then(merge({ ids }));
};

//25 at a time is ddb limit. For more than 25, make multiple requests
const bulkBatches = async ({ ddb, db, docs }) => {
  const buckets = [];
  const ids = [];
  let UnprocessedItems = {};

  const bucketSize = 25;
  for (let i = 0; i < docs.length; i += bucketSize) {
    buckets.push([...docs.slice(i, i + bucketSize)]);
  }

  return buckets
    .reduce(
      (accPromise, nextBucket) =>
        accPromise.then(async () => {
          const res = await oneBatch({ ddb, db, docs: nextBucket });
          ids.push(...res.ids);
          UnprocessedItems = { ...UnprocessedItems, ...res.UnprocessedItems };
        }),
      Promise.resolve()
    )
    .then(() => ({ ids, UnprocessedItems }));
};

export const doBulkUpdate = ({ ddb, db, docs }) =>
  docs.length <= 25
    ? oneBatch({ ddb, db, docs })
    : bulkBatches({ ddb, db, docs });
