import { drizzle } from "drizzle-orm/mysql2";

import { poolConn, poolConn2, poolConn3, poolConn4, poolConn5 } from "../config/conn";
import * as schema from './schema'
import * as schema2 from './schema2'
import * as schema3 from './schema3'
import * as schema4 from './schema4'
import * as schema5 from './schema5'

export const db = drizzle({ client: poolConn, mode: 'default', schema })
export const db2 = drizzle({ client: poolConn2, mode: 'default', schema: schema2 })
export const db3 = drizzle({ client: poolConn3, mode: 'default', schema: schema3 })
export const db4 = drizzle({ client: poolConn4, mode: "default", schema: schema4 });
export const db5 = drizzle({ client: poolConn5, mode: "default", schema: schema5 });
