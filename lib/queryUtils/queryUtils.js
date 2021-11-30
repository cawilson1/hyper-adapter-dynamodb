import { marshall, R, unmarshall } from "../../deps.js";
import { _throw } from "../utils.js";
import { TableName } from "../constants.js";
import { queryAll } from "../listDocHelpers.js";

const {
  always,
  compose,
  find,
  flip,
  identity,
  isEmpty,
  prop,
  keys,
  map,
  merge,
  slice,
} = R;

//real indexes are not feasible here.
//mimic the functionality of indexes by keeping track of indexes with fields in meta doc
export const retrieveIndexFields = async ({ ddb, db, use_index }) =>
  use_index
    ? await ddb
        .getItem({
          TableName,
          Key: marshall({ pk: db, sk: "meta" }),
        })
        .then(prop("Item"))
        .then(unmarshall)
        .then(prop("indexes"))
        .then(find((el) => el[use_index]))
        .then(prop(use_index))
    : undefined;

//dynamodb partiql only supports sorting via primary key values
//so we'll have to handle sorting on adapter
const querySort = ({ value, sort }) => {
  const sortVal = sort ? keys(sort[0])[0] : "";

  const isSortDesc = sort?.[0]?.[sortVal] === "DESC";
  const sortDesc = (a, b) => b["title"].localeCompare(a["title"]);
  const sortAsc = flip(sortDesc);
  return sort
    ? isSortDesc
      ? R.sort(sortDesc)(value)
      : R.sort(sortAsc)(value)
    : value;
};

export const queryUtils = ({ limit, sort }) => {
  const s = limit ? slice(0, limit) : identity;
  const unmarshallResult = compose(map(unmarshall), s);
  const unmarshallResponse = (result) =>
    result?.Items ? unmarshallResult(result.Items) : unmarshallResult(result);
  const doSort = (value) => querySort({ value, sort });
  return { unmarshallResponse, doSort };
};
