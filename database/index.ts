import { drizzle } from "drizzle-orm/mysql2";

import { poolConn, poolConn2, poolConn3 } from "../config/conn";
import * as schema from './schema'
import * as schema2 from './schema2'
import * as schema3 from './schema3'

export const db = drizzle({ client: poolConn, mode: 'default', schema })
export const db2 = drizzle({ client: poolConn2, mode: 'default', schema: schema2 })
export const db3 = drizzle({ client: poolConn3, mode: 'default', schema: schema3 })