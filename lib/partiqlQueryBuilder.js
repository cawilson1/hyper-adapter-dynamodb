import { R } from "../deps.js";
import { TableName } from "./constants.js";
import { _throw } from "./utils.js";

const { equals, filter, find, includes, keys, map } = R;

// selectors
//empty ->
//[k]:v -> SELECT * WHERE [k]=v
//$or -> OR
//$and -> AND
//[name] WHERE [cond] [name]
//fields SELECT [field1], [field2] WHERE [cond]
//$gt -> >
//$lt -> <
//$gte -> >=
//$lte -> <=
//sort (DESC or ASC) -> ORDER BY [field] ASC
const reservedSelectors = ["$lte", "$gte", "$lt", "$gt", "$and", "$or"];
const inReservedSelectors = (el) => find(equals(el))(reservedSelectors);

//if(fields) columnQuery
//else starQuery

const columnQuery = (fields, TableName) => {
  console.log("querying");
  return (
    fields.reduce((acc, el) => `${acc} ${el},`, "SELECT").replace(/(,$)/, "") +
    ` FROM "${TableName}" `
  );
};

export const queryBuilder = ({ selector, fields, pk, index }) => {
  let query = "";
  const includesIndex = (el) => includes(el, index);
  if (index && fields) {
    const commonFields = filter(includesIndex)(fields);
    if(commonFields.length===0) _throw("There are no common fields in your index and your request")
    query = columnQuery(commonFields, TableName);
  } else if (index) query = columnQuery(index, TableName);
  else if (fields) query = columnQuery(fields, TableName);
  else query = `SELECT * FROM "${TableName}"`;

  const selectors = map(inReservedSelectors)(keys(selector));

  //check for and and or, these signify something different from others
  const firstKey = keys(selector)[0];
  console.log(firstKey);
  query += ` WHERE pk = '${pk}' AND ${firstKey} = '${selector[firstKey]}'`;

  // console.log("reserved selectors", map(inReservedSelectors)(keys(selector)))
  console.log("THIS IS THE QUERY", query);
  return query;
};

// queryBuilder({
//   selector: { type: "album" },
//   TableName: "test",
// });
// queryBuilder({
//     selector: { type: "album" }, fields: ["id", "band", "toast"], TableName: "test"
// })
// queryBuilder({ selector: { type: "album" }, TableName: "test" })
// queryBuilder({ selector: { $or: { year: { "$gt": "1970" } }, year: { "$lt": "1980" } }, TableName: "test" })
// queryBuilder({ selector: { year: { "$gt": "1970" } }, TableName: "test" })
