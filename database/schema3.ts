import { mysqlSchema, varchar, decimal, index } from "drizzle-orm/mysql-core";
import { relations } from "drizzle-orm";

export const hadoop2Schema = mysqlSchema("hadoop2");

export const dynamicByuTable = (year: string, month: string) => {
    return hadoop2Schema.table(`byu_${year}${month}`, {
        msisdn: varchar({ length: 18 }),
        periode: varchar({ length: 10 }),
        eventDate: varchar('event_date', { length: 18 }),
        areaSales: varchar('area_sales', { length: 20 }),
        regionSales: varchar('region_sales', { length: 35 }),
        branch: varchar('branch', { length: 35 }),
        clusterSales: varchar('cluster_sales', { length: 35 }),
        kabupaten: varchar('kabupaten', { length: 40 }),
        kecamatan: varchar('kecamatan', { length: 45 }),
        brand: varchar('brand', { length: 45 }),
        l1Name: varchar('l1_name', { length: 100 }),
        offerName: varchar('offer_name', { length: 100 }),
        source: varchar('source', { length: 100 }),
        rev: varchar('rev', { length: 22 }),
        trx: varchar('trx', { length: 22 }),
        subs: varchar('subs', { length: 22 }),
    })
}