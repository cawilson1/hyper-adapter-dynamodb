export async function dbQuery(tableName) {
  const result = await connection.promise().query(
    `SELECT * 
      FROM ?`,
    [tableName]
  );

  return result;
}
