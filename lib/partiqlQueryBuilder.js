import { R } from "../deps.js";

const { find } = R

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

//if(fields) columnQuery
//else starQuery

export const columnQuery = (fields, tableName) => {
    console.log("querying")
    return fields.reduce((acc, el) => `${acc} ${el},`, "SELECT").replace(/(,$)/, "") + `
    FROM ${tableName}
    `
}


export const queryBuilder = ({ selector, fields, tableName }) => {
    let query = ""
    if (fields) query = columnQuery(fields, tableName)
    else query = `SELECT * FROM ${tableName}
    `
    console.log(query)
}

queryBuilder({
    selector: { type: "album" }, fields: ["id", "band", "toast"], tableName: "test"
})
queryBuilder({ selector: { type: "album" }, tableName: "test" })