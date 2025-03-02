import { Elysia, t } from "elysia";
import { swagger } from '@elysiajs/swagger'
import { and, asc, between, eq, inArray, isNotNull, like, not, notInArray, sql } from "drizzle-orm";
import { subMonths, subDays, format, subYears } from 'date-fns'
import { index } from "drizzle-orm/mysql-core";

import { db as conn, db2 as conn2, db3 as conn3, db4 as conn4, db5 as conn5 } from '../database'
import { dynamicCbProfileTable, dynamicResumeRevenuePumaTable, dynamicRevenueCVMTable } from "../database/schema2";
import { dynamicByuTable } from "../database/schema3";
import { table } from "../database/schema";
import { dynamicMergeNewSalesPumaTable } from "../database/schema4";
import { dynamicRevenueSATable } from "../database/schema5";

const { regionals, branches, clusters, kabupatens, subbranches, revenueGrosses, revenueByu, revenueCVM, revenueNewSales, payingLOS_01, revenueSA, payingSubs } = table

new Elysia({ prefix: '/api', serve: { idleTimeout: 255 } })
    .decorate('db', conn)
    .decorate('db2', conn2)
    .decorate('db3', conn3)
    .decorate('db4', conn4)
    .decorate('db5', conn5)
    .use(swagger())
    .get('/areas', async ({ db }) => {
        const data = await db.query.regionals.findMany({
            with: {
                branches: {
                    with: {
                        subbranches: {
                            with: {
                                clusters: {
                                    with: {
                                        kabupatens: true
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
        return Response.json({ data: data }, { status: 200 })
    })
    .get('/revenue-byu', async ({ db, db3, query }) => {
        const { date } = query
        const selectedDate = date ? new Date(date) : new Date()
        const month = (subDays(selectedDate, 2).getMonth() + 1).toString()

        // KOLOM DINAMIS UNTUK MEMILIH ANTARA KOLOM `m1-m12`
        const monthColumn = `m${month}` as keyof typeof revenueByu.$inferSelect

        // VARIABLE TANGGAL UNTUK IMPORT TABEL SECARA DINAMIS
        const latestDataDate = subDays(selectedDate, 2);

        const currMonth = format(latestDataDate, 'MM')
        const currYear = format(latestDataDate, 'yyyy')
        const isPrevMonthLastYear = currMonth === '01'
        const prevMonth = isPrevMonthLastYear ? '12' : format(subMonths(latestDataDate, 1), 'MM')
        const prevMonthYear = isPrevMonthLastYear ? format(subYears(latestDataDate, 1), 'yyyy') : format(latestDataDate, 'yyyy')
        const prevYear = format(subYears(latestDataDate, 1), 'yyyy')

        // TABEL DINAMIS
        const currRevByu = dynamicByuTable(currYear, currMonth)
        const prevMonthRevByu = dynamicByuTable(prevMonthYear, prevMonth)
        const prevYearCurrMonthRevByu = dynamicByuTable(prevYear, currMonth)

        // VARIABLE TANGGAL
        const firstDayOfCurrMonth = format(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 'yyyy-MM-dd')
        const firstDayOfPrevMonth = format(subMonths(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const firstDayOfPrevYearCurrMonth = format(subYears(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const currDate = format(latestDataDate, 'yyyy-MM-dd')
        const prevDate = format(subMonths(latestDataDate, 1), 'yyyy-MM-dd')
        const prevYearCurrDate = format(subYears(latestDataDate, 1), 'yyyy-MM-dd')

        const sq2 = db3
            .select({
                regionName: sql<string>`CASE WHEN ${currRevByu.regionSales} IN ('MALUKU DAN PAPUA', 'PUMA') THEN 'PUMA' END`.as('regionName'),
                branchName: sql<string>`
             CASE
                 WHEN ${currRevByu.kabupaten} IN (
                     'AMBON',
                     'KOTA AMBON',
                     'MALUKU TENGAH',
                     'SERAM BAGIAN TIMUR',
                     'KEPULAUAN ARU',
                     'KOTA TUAL',
                     'MALUKU BARAT DAYA',
                     'MALUKU TENGGARA',
                     'MALUKU TENGGARA BARAT',
                     'BURU',
                     'BURU SELATAN',
                     'SERAM BAGIAN BARAT',
                     'KEPULAUAN TANIMBAR'
                 ) THEN 'AMBON'
                 WHEN ${currRevByu.kabupaten} IN (
                     'KOTA JAYAPURA',
                     'JAYAPURA',
                     'KEEROM',
                     'MAMBERAMO RAYA',
                     'SARMI',
                     'BIAK',
                     'BIAK NUMFOR',
                     'KEPULAUAN YAPEN',
                     'SUPIORI',
                     'WAROPEN',
                     'JAYAWIJAYA',
                     'LANNY JAYA',
                     'MAMBERAMO TENGAH',
                     'NDUGA',
                     'PEGUNUNGAN BINTANG',
                     'TOLIKARA',
                     'YAHUKIMO',
                     'YALIMO'
                 ) THEN 'JAYAPURA'
                 WHEN ${currRevByu.kabupaten} IN (
                     'MANOKWARI',
                     'FAKFAK',
                     'FAK FAK',
                     'KAIMANA',
                     'MANOKWARI SELATAN',
                     'PEGUNUNGAN ARFAK',
                     'TELUK BINTUNI',
                     'TELUK WONDAMA',
                     'KOTA SORONG',
                     'MAYBRAT',
                     'RAJA AMPAT',
                     'SORONG',
                     'SORONG SELATAN',
                     'TAMBRAUW'
                 ) THEN 'SORONG'
                 WHEN ${currRevByu.kabupaten} IN (
                     'ASMAT',
                     'BOVEN DIGOEL',
                     'MAPPI',
                     'MERAUKE',
                     'INTAN JAYA',
                     'MIMIKA',
                     'PUNCAK',
                     'PUNCAK JAYA',
                     'TIMIKA',
                     'DEIYAI',
                     'DOGIYAI',
                     'NABIRE',
                     'PANIAI'
                 ) THEN 'TIMIKA'
                 ELSE NULL
             END
                    `.as('branchName'),
                subbranchName: sql<string>`
             CASE
                 WHEN ${currRevByu.kabupaten} IN (
                     'AMBON',
                     'KOTA AMBON',
                     'MALUKU TENGAH',
                     'SERAM BAGIAN TIMUR'
                 ) THEN 'AMBON'
                 WHEN ${currRevByu.kabupaten} IN (
                     'KEPULAUAN ARU',
                     'KOTA TUAL',
                     'MALUKU BARAT DAYA',
                     'MALUKU TENGGARA',
                     'MALUKU TENGGARA BARAT',
                     'KEPULAUAN TANIMBAR'
                 ) THEN 'KEPULAUAN AMBON'
                 WHEN ${currRevByu.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
                 WHEN ${currRevByu.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
                 WHEN ${currRevByu.kabupaten} IN (
                     'JAYAPURA',
                     'KEEROM',
                     'MAMBERAMO RAYA',
                     'SARMI',
                     'BIAK',
                     'BIAK NUMFOR',
                     'KEPULAUAN YAPEN',
                     'SUPIORI',
                     'WAROPEN',
                     'JAYAWIJAYA',
                     'LANNY JAYA',
                     'MAMBERAMO TENGAH',
                     'NDUGA',
                     'PEGUNUNGAN BINTANG',
                     'TOLIKARA',
                     'YAHUKIMO',
                     'YALIMO'
                 ) THEN 'SENTANI'
                 WHEN ${currRevByu.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
                 WHEN ${currRevByu.kabupaten} IN (
                     'FAKFAK',
                     'FAK FAK',
                     'KAIMANA',
                     'MANOKWARI SELATAN',
                     'PEGUNUNGAN ARFAK',
                     'TELUK BINTUNI',
                     'TELUK WONDAMA'
                 ) THEN 'MANOKWARI OUTER'
                 WHEN ${currRevByu.kabupaten} IN (
                     'KOTA SORONG',
                     'MAYBRAT',
                     'RAJA AMPAT',
                     'SORONG',
                     'SORONG SELATAN',
                     'TAMBRAUW'
                 ) THEN 'SORONG RAJA AMPAT'
                 WHEN ${currRevByu.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
                 WHEN ${currRevByu.kabupaten} IN (
                     'INTAN JAYA',
                     'MIMIKA',
                     'PUNCAK',
                     'PUNCAK JAYA',
                     'TIMIKA'
                 ) THEN 'MIMIKA'
                 WHEN ${currRevByu.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
                 ELSE NULL
             END
                    `.as('subbranchName'),
                clusterName: sql<string>`
             CASE
                 WHEN ${currRevByu.kabupaten} IN (
                     'KOTA AMBON',
                     'MALUKU TENGAH',
                     'SERAM BAGIAN TIMUR'
                 ) THEN 'AMBON'
                 WHEN ${currRevByu.kabupaten} IN (
                     'KEPULAUAN ARU',
                     'KOTA TUAL',
                     'MALUKU BARAT DAYA',
                     'MALUKU TENGGARA',
                     'MALUKU TENGGARA BARAT',
                     'KEPULAUAN TANIMBAR'
                 ) THEN 'KEPULAUAN TUAL'
                 WHEN ${currRevByu.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
                 WHEN ${currRevByu.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
                 WHEN ${currRevByu.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
                 WHEN ${currRevByu.kabupaten} IN (
                     'BIAK',
                     'BIAK NUMFOR',
                     'KEPULAUAN YAPEN',
                     'SUPIORI',
                     'WAROPEN'
                 ) THEN 'NEW BIAK NUMFOR'
                 WHEN ${currRevByu.kabupaten} IN (
                     'JAYAWIJAYA',
                     'LANNY JAYA',
                     'MAMBERAMO TENGAH',
                     'NDUGA',
                     'PEGUNUNGAN BINTANG',
                     'TOLIKARA',
                     'YAHUKIMO',
                     'YALIMO'
                 ) THEN 'PAPUA PEGUNUNGAN'
                 WHEN ${currRevByu.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
                 WHEN ${currRevByu.kabupaten} IN (
                     'FAKFAK',
                     'FAK FAK',
                     'KAIMANA',
                     'MANOKWARI SELATAN',
                     'PEGUNUNGAN ARFAK',
                     'TELUK BINTUNI',
                     'TELUK WONDAMA'
                 ) THEN 'MANOKWARI OUTER'
                 WHEN ${currRevByu.kabupaten} IN (
                     'KOTA SORONG',
                     'MAYBRAT',
                     'RAJA AMPAT',
                     'SORONG',
                     'SORONG SELATAN',
                     'TAMBRAUW'
                 ) THEN 'NEW SORONG RAJA AMPAT'
                 WHEN ${currRevByu.kabupaten} IN (
                     'INTAN JAYA',
                     'MIMIKA',
                     'PUNCAK',
                     'PUNCAK JAYA',
                     'TIMIKA'
                 ) THEN 'MIMIKA PUNCAK'
                 WHEN ${currRevByu.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
                 WHEN ${currRevByu.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
                 ELSE NULL
             END
                    `.as('clusterName'),
                kabupaten: currRevByu.kabupaten,
                rev: currRevByu.rev,
            })
            .from(currRevByu)
            .where(between(currRevByu.eventDate, firstDayOfCurrMonth, currDate))
            .as('sq2')

        const sq3 = db3
            .select({
                regionName: sql<string>`CASE WHEN ${prevMonthRevByu.regionSales} IN ('MALUKU DAN PAPUA', 'PUMA') THEN 'PUMA' END`.as('regionName'),
                branchName: sql<string>`
             CASE
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'AMBON',
                     'KOTA AMBON',
                     'MALUKU TENGAH',
                     'SERAM BAGIAN TIMUR',
                     'KEPULAUAN ARU',
                     'KOTA TUAL',
                     'MALUKU BARAT DAYA',
                     'MALUKU TENGGARA',
                     'MALUKU TENGGARA BARAT',
                     'BURU',
                     'BURU SELATAN',
                     'SERAM BAGIAN BARAT',
                     'KEPULAUAN TANIMBAR'
                 ) THEN 'AMBON'
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'KOTA JAYAPURA',
                     'JAYAPURA',
                     'KEEROM',
                     'MAMBERAMO RAYA',
                     'SARMI',
                     'BIAK',
                     'BIAK NUMFOR',
                     'KEPULAUAN YAPEN',
                     'SUPIORI',
                     'WAROPEN',
                     'JAYAWIJAYA',
                     'LANNY JAYA',
                     'MAMBERAMO TENGAH',
                     'NDUGA',
                     'PEGUNUNGAN BINTANG',
                     'TOLIKARA',
                     'YAHUKIMO',
                     'YALIMO'
                 ) THEN 'JAYAPURA'
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'MANOKWARI',
                     'FAKFAK',
                     'FAK FAK',
                     'KAIMANA',
                     'MANOKWARI SELATAN',
                     'PEGUNUNGAN ARFAK',
                     'TELUK BINTUNI',
                     'TELUK WONDAMA',
                     'KOTA SORONG',
                     'MAYBRAT',
                     'RAJA AMPAT',
                     'SORONG',
                     'SORONG SELATAN',
                     'TAMBRAUW'
                 ) THEN 'SORONG'
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'ASMAT',
                     'BOVEN DIGOEL',
                     'MAPPI',
                     'MERAUKE',
                     'INTAN JAYA',
                     'MIMIKA',
                     'PUNCAK',
                     'PUNCAK JAYA',
                     'TIMIKA',
                     'DEIYAI',
                     'DOGIYAI',
                     'NABIRE',
                     'PANIAI'
                 ) THEN 'TIMIKA'
                 ELSE NULL
             END
                    `.as('branchName'),
                subbranchName: sql<string>`
             CASE
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'AMBON',
                     'KOTA AMBON',
                     'MALUKU TENGAH',
                     'SERAM BAGIAN TIMUR'
                 ) THEN 'AMBON'
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'KEPULAUAN ARU',
                     'KOTA TUAL',
                     'MALUKU BARAT DAYA',
                     'MALUKU TENGGARA',
                     'MALUKU TENGGARA BARAT',
                     'KEPULAUAN TANIMBAR'
                 ) THEN 'KEPULAUAN AMBON'
                 WHEN ${prevMonthRevByu.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
                 WHEN ${prevMonthRevByu.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'JAYAPURA',
                     'KEEROM',
                     'MAMBERAMO RAYA',
                     'SARMI',
                     'BIAK',
                     'BIAK NUMFOR',
                     'KEPULAUAN YAPEN',
                     'SUPIORI',
                     'WAROPEN',
                     'JAYAWIJAYA',
                     'LANNY JAYA',
                     'MAMBERAMO TENGAH',
                     'NDUGA',
                     'PEGUNUNGAN BINTANG',
                     'TOLIKARA',
                     'YAHUKIMO',
                     'YALIMO'
                 ) THEN 'SENTANI'
                 WHEN ${prevMonthRevByu.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'FAKFAK',
                     'FAK FAK',
                     'KAIMANA',
                     'MANOKWARI SELATAN',
                     'PEGUNUNGAN ARFAK',
                     'TELUK BINTUNI',
                     'TELUK WONDAMA'
                 ) THEN 'MANOKWARI OUTER'
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'KOTA SORONG',
                     'MAYBRAT',
                     'RAJA AMPAT',
                     'SORONG',
                     'SORONG SELATAN',
                     'TAMBRAUW'
                 ) THEN 'SORONG RAJA AMPAT'
                 WHEN ${prevMonthRevByu.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'INTAN JAYA',
                     'MIMIKA',
                     'PUNCAK',
                     'PUNCAK JAYA',
                     'TIMIKA'
                 ) THEN 'MIMIKA'
                 WHEN ${prevMonthRevByu.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
                 ELSE NULL
             END
                    `.as('subbranchName'),
                clusterName: sql<string>`
             CASE
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'KOTA AMBON',
                     'MALUKU TENGAH',
                     'SERAM BAGIAN TIMUR'
                 ) THEN 'AMBON'
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'KEPULAUAN ARU',
                     'KOTA TUAL',
                     'MALUKU BARAT DAYA',
                     'MALUKU TENGGARA',
                     'MALUKU TENGGARA BARAT',
                     'KEPULAUAN TANIMBAR'
                 ) THEN 'KEPULAUAN TUAL'
                 WHEN ${prevMonthRevByu.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
                 WHEN ${prevMonthRevByu.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
                 WHEN ${prevMonthRevByu.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'BIAK',
                     'BIAK NUMFOR',
                     'KEPULAUAN YAPEN',
                     'SUPIORI',
                     'WAROPEN'
                 ) THEN 'NEW BIAK NUMFOR'
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'JAYAWIJAYA',
                     'LANNY JAYA',
                     'MAMBERAMO TENGAH',
                     'NDUGA',
                     'PEGUNUNGAN BINTANG',
                     'TOLIKARA',
                     'YAHUKIMO',
                     'YALIMO'
                 ) THEN 'PAPUA PEGUNUNGAN'
                 WHEN ${prevMonthRevByu.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'FAKFAK',
                     'FAK FAK',
                     'KAIMANA',
                     'MANOKWARI SELATAN',
                     'PEGUNUNGAN ARFAK',
                     'TELUK BINTUNI',
                     'TELUK WONDAMA'
                 ) THEN 'MANOKWARI OUTER'
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'KOTA SORONG',
                     'MAYBRAT',
                     'RAJA AMPAT',
                     'SORONG',
                     'SORONG SELATAN',
                     'TAMBRAUW'
                 ) THEN 'NEW SORONG RAJA AMPAT'
                 WHEN ${prevMonthRevByu.kabupaten} IN (
                     'INTAN JAYA',
                     'MIMIKA',
                     'PUNCAK',
                     'PUNCAK JAYA',
                     'TIMIKA'
                 ) THEN 'MIMIKA PUNCAK'
                 WHEN ${prevMonthRevByu.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
                 WHEN ${prevMonthRevByu.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
                 ELSE NULL
             END
                    `.as('clusterName'),
                kabupaten: prevMonthRevByu.kabupaten,
                rev: prevMonthRevByu.rev,
            })
            .from(prevMonthRevByu)
            .where(between(prevMonthRevByu.eventDate, firstDayOfPrevMonth, prevDate))
            .as('sq3')

        const sq4 = db3
            .select({
                regionName: sql<string>`CASE WHEN ${prevYearCurrMonthRevByu.regionSales} IN ('MALUKU DAN PAPUA', 'PUMA') THEN 'PUMA' END`.as('regionName'),
                branchName: sql<string>`
             CASE
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'AMBON',
                     'KOTA AMBON',
                     'MALUKU TENGAH',
                     'SERAM BAGIAN TIMUR',
                     'KEPULAUAN ARU',
                     'KOTA TUAL',
                     'MALUKU BARAT DAYA',
                     'MALUKU TENGGARA',
                     'MALUKU TENGGARA BARAT',
                     'BURU',
                     'BURU SELATAN',
                     'SERAM BAGIAN BARAT',
                     'KEPULAUAN TANIMBAR'
                 ) THEN 'AMBON'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'KOTA JAYAPURA',
                     'JAYAPURA',
                     'KEEROM',
                     'MAMBERAMO RAYA',
                     'SARMI',
                     'BIAK',
                     'BIAK NUMFOR',
                     'KEPULAUAN YAPEN',
                     'SUPIORI',
                     'WAROPEN',
                     'JAYAWIJAYA',
                     'LANNY JAYA',
                     'MAMBERAMO TENGAH',
                     'NDUGA',
                     'PEGUNUNGAN BINTANG',
                     'TOLIKARA',
                     'YAHUKIMO',
                     'YALIMO'
                 ) THEN 'JAYAPURA'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'MANOKWARI',
                     'FAKFAK',
                     'FAK FAK',
                     'KAIMANA',
                     'MANOKWARI SELATAN',
                     'PEGUNUNGAN ARFAK',
                     'TELUK BINTUNI',
                     'TELUK WONDAMA',
                     'KOTA SORONG',
                     'MAYBRAT',
                     'RAJA AMPAT',
                     'SORONG',
                     'SORONG SELATAN',
                     'TAMBRAUW'
                 ) THEN 'SORONG'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'ASMAT',
                     'BOVEN DIGOEL',
                     'MAPPI',
                     'MERAUKE',
                     'INTAN JAYA',
                     'MIMIKA',
                     'PUNCAK',
                     'PUNCAK JAYA',
                     'TIMIKA',
                     'DEIYAI',
                     'DOGIYAI',
                     'NABIRE',
                     'PANIAI'
                 ) THEN 'TIMIKA'
                 ELSE NULL
             END
                    `.as('branchName'),
                subbranchName: sql<string>`
             CASE
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'AMBON',
                     'KOTA AMBON',
                     'MALUKU TENGAH',
                     'SERAM BAGIAN TIMUR'
                 ) THEN 'AMBON'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'KEPULAUAN ARU',
                     'KOTA TUAL',
                     'MALUKU BARAT DAYA',
                     'MALUKU TENGGARA',
                     'MALUKU TENGGARA BARAT',
                     'KEPULAUAN TANIMBAR'
                 ) THEN 'KEPULAUAN AMBON'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'JAYAPURA',
                     'KEEROM',
                     'MAMBERAMO RAYA',
                     'SARMI',
                     'BIAK',
                     'BIAK NUMFOR',
                     'KEPULAUAN YAPEN',
                     'SUPIORI',
                     'WAROPEN',
                     'JAYAWIJAYA',
                     'LANNY JAYA',
                     'MAMBERAMO TENGAH',
                     'NDUGA',
                     'PEGUNUNGAN BINTANG',
                     'TOLIKARA',
                     'YAHUKIMO',
                     'YALIMO'
                 ) THEN 'SENTANI'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'FAKFAK',
                     'FAK FAK',
                     'KAIMANA',
                     'MANOKWARI SELATAN',
                     'PEGUNUNGAN ARFAK',
                     'TELUK BINTUNI',
                     'TELUK WONDAMA'
                 ) THEN 'MANOKWARI OUTER'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'KOTA SORONG',
                     'MAYBRAT',
                     'RAJA AMPAT',
                     'SORONG',
                     'SORONG SELATAN',
                     'TAMBRAUW'
                 ) THEN 'SORONG RAJA AMPAT'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'INTAN JAYA',
                     'MIMIKA',
                     'PUNCAK',
                     'PUNCAK JAYA',
                     'TIMIKA'
                 ) THEN 'MIMIKA'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
                 ELSE NULL
             END
                    `.as('subbranchName'),
                clusterName: sql<string>`
             CASE
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'KOTA AMBON',
                     'MALUKU TENGAH',
                     'SERAM BAGIAN TIMUR'
                 ) THEN 'AMBON'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'KEPULAUAN ARU',
                     'KOTA TUAL',
                     'MALUKU BARAT DAYA',
                     'MALUKU TENGGARA',
                     'MALUKU TENGGARA BARAT',
                     'KEPULAUAN TANIMBAR'
                 ) THEN 'KEPULAUAN TUAL'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'BIAK',
                     'BIAK NUMFOR',
                     'KEPULAUAN YAPEN',
                     'SUPIORI',
                     'WAROPEN'
                 ) THEN 'NEW BIAK NUMFOR'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'JAYAWIJAYA',
                     'LANNY JAYA',
                     'MAMBERAMO TENGAH',
                     'NDUGA',
                     'PEGUNUNGAN BINTANG',
                     'TOLIKARA',
                     'YAHUKIMO',
                     'YALIMO'
                 ) THEN 'PAPUA PEGUNUNGAN'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'FAKFAK',
                     'FAK FAK',
                     'KAIMANA',
                     'MANOKWARI SELATAN',
                     'PEGUNUNGAN ARFAK',
                     'TELUK BINTUNI',
                     'TELUK WONDAMA'
                 ) THEN 'MANOKWARI OUTER'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'KOTA SORONG',
                     'MAYBRAT',
                     'RAJA AMPAT',
                     'SORONG',
                     'SORONG SELATAN',
                     'TAMBRAUW'
                 ) THEN 'NEW SORONG RAJA AMPAT'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN (
                     'INTAN JAYA',
                     'MIMIKA',
                     'PUNCAK',
                     'PUNCAK JAYA',
                     'TIMIKA'
                 ) THEN 'MIMIKA PUNCAK'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
                 WHEN ${prevYearCurrMonthRevByu.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
                 ELSE NULL
             END
                    `.as('clusterName'),
                kabupaten: prevYearCurrMonthRevByu.kabupaten,
                rev: prevYearCurrMonthRevByu.rev,
            })
            .from(prevYearCurrMonthRevByu)
            .where(between(prevYearCurrMonthRevByu.eventDate, firstDayOfPrevYearCurrMonth, prevYearCurrDate))
            .as('sq4')

        // QUERY UNTUK TARGET BULAN INI
        const p1 = db
            .select({
                id: regionals.id,
                region: regionals.regional,
                branch: branches.branchNew,
                subbranch: subbranches.subbranchNew,
                cluster: clusters.cluster,
                kabupaten: kabupatens.kabupaten,
                currMonthTargetRev: sql<number>`CAST(SUM(${revenueByu[monthColumn]}) AS DOUBLE PRECISION)`.as('currMonthTargetRev')
            })
            .from(regionals)
            .leftJoin(branches, eq(regionals.id, branches.regionalId))
            .leftJoin(subbranches, eq(branches.id, subbranches.branchId))
            .leftJoin(clusters, eq(subbranches.id, clusters.subbranchId))
            .leftJoin(kabupatens, eq(clusters.id, kabupatens.clusterId))
            .leftJoin(revenueByu, eq(kabupatens.id, revenueByu.kabupatenId))
            .groupBy(
                regionals.regional,
                branches.branchNew,
                subbranches.subbranchNew,
                clusters.cluster,
                kabupatens.kabupaten
            )
            .orderBy(asc(regionals.regional), asc(branches.branchNew), asc(subbranches.subbranchNew), asc(clusters.cluster), asc(kabupatens.kabupaten))
            .prepare()

        //  QUERY UNTUK MENDAPAT CURRENT MONTH REVENUE (Mtd)
        const p2 = db3
            .select({
                region: sql<string>`${sq2.regionName}`.as('region'),
                branch: sql<string>`${sq2.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq2.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq2.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq2.kabupaten}`.as('kabupaten'),
                currMonthKabupatenRev: sql<number>`SUM(${sq2.rev})`.as('currMonthKabupatenRev'),
                currMonthClusterRev: sql<number>`SUM(SUM(${sq2.rev})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName}, ${sq2.subbranchName}, ${sq2.clusterName})`.as('currMonthClusterRev'),
                currMonthSubbranchRev: sql<number>`SUM(SUM(${sq2.rev})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName}, ${sq2.subbranchName})`.as('currMonthSubbranchRev'),
                currMonthBranchRev: sql<number>`SUM(SUM(${sq2.rev})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName})`.as('currMonthBranchRev'),
                currMonthRegionalRev: sql<number>`SUM(SUM(${sq2.rev})) OVER (PARTITION BY ${sq2.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq2)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        // QUERY UNTUK MENDAPAT PREV MONTH REVENUE
        const p3 = db3
            .select({
                region: sql<string>`${sq3.regionName}`.as('region'),
                branch: sql<string>`${sq3.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq3.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq3.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq3.kabupaten}`.as('kabupaten'),
                prevMonthKabupatenRev: sql<number>`SUM(${sq3.rev})`.as('currMonthKabupatenRev'),
                prevMonthClusterRev: sql<number>`SUM(SUM(${sq3.rev})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName}, ${sq3.subbranchName}, ${sq3.clusterName})`.as('currMonthClusterRev'),
                prevMonthSubbranchRev: sql<number>`SUM(SUM(${sq3.rev})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName}, ${sq3.subbranchName})`.as('currMonthSubbranchRev'),
                prevMonthBranchRev: sql<number>`SUM(SUM(${sq3.rev})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName})`.as('currMonthBranchRev'),
                prevMonthRegionalRev: sql<number>`SUM(SUM(${sq3.rev})) OVER (PARTITION BY ${sq3.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq3)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        // QUERY UNTUK MENDAPAT PREV YEAR CURR MONTH REVENUE
        const p4 = db3
            .select({
                region: sql<string>`${sq4.regionName}`.as('region'),
                branch: sql<string>`${sq4.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq4.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq4.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq4.kabupaten}`.as('kabupaten'),
                prevYearCurrMonthKabupatenRev: sql<number>`SUM(${sq4.rev})`.as('currMonthKabupatenRev'),
                prevYearCurrMonthClusterRev: sql<number>`SUM(SUM(${sq4.rev})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName}, ${sq4.subbranchName}, ${sq4.clusterName})`.as('currMonthClusterRev'),
                prevYearCurrMonthSubbranchRev: sql<number>`SUM(SUM(${sq4.rev})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName}, ${sq4.subbranchName})`.as('currMonthSubbranchRev'),
                prevYearCurrMonthBranchRev: sql<number>`SUM(SUM(${sq4.rev})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName})`.as('currMonthBranchRev'),
                prevYearCurrMonthRegionalRev: sql<number>`SUM(SUM(${sq4.rev})) OVER (PARTITION BY ${sq4.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq4)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        // QUERY UNTUK YtD 2025

        const [targetRevenue, currMonthRevenue, prevMonthRevenue, prevYearCurrMonthRevenue] = await Promise.all([
            p1.execute(),
            p2.execute(),
            p3.execute(),
            p4.execute()
        ])
        // /var/lib/backup_mysql_2025/
        const regionalsMap = new Map();

        targetRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthTarget += Number(row.currMonthTargetRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize kabupaten if it doesn't exist
            cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: Number(row.currMonthTargetRev),
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
        })

        currMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthRevenue = Number(row.currMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.currMonthRevenue = Number(row.currMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.currMonthRevenue = Number(row.currMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.currMonthRevenue = Number(row.currMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));

            kabupaten.currMonthRevenue = Number(row.currMonthKabupatenRev)
        })

        prevMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevMonthRevenue = Number(row.prevMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.prevMonthRevenue = Number(row.prevMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.prevMonthRevenue = Number(row.prevMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevMonthRevenue = Number(row.prevMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
            kabupaten.prevMonthRevenue = Number(row.prevMonthKabupatenRev)
        })

        prevYearCurrMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
            kabupaten.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthKabupatenRev)
        })

        const finalDataRevenue = Array.from(regionalsMap.values()).map((regional: any) => ({
            ...regional,
            branches: Array.from(regional.branches.values()).map((branch: any) => ({
                ...branch,
                subbranches: Array.from(branch.subbranches.values()).map((subbranch: any) => ({
                    ...subbranch,
                    clusters: Array.from(subbranch.clusters.values().map((cluster: any) => ({
                        ...cluster,
                        kabupatens: Array.from(cluster.kabupatens.values())
                    }))),
                })),
            })),
        }));

        return Response.json({ data: finalDataRevenue }, { status: 200 })
    }, {
        query: t.Object({
            date: t.Optional(t.Date()),
            branch: t.Optional(t.String()),
            subbranch: t.Optional(t.String()),
            cluster: t.Optional(t.String()),
            kabupaten: t.Optional(t.String())
        })
    })
    .get('/revenue-grosses', async ({ db, db2, query }) => {
        const { date } = query
        const selectedDate = date ? new Date(date) : new Date()
        const month = (subDays(selectedDate, 2).getMonth() + 1).toString()

        // KOLOM DINAMIS UNTUK MEMILIH ANTARA KOLOM `m1-m12`
        const monthColumn = `m${month}` as keyof typeof revenueGrosses.$inferSelect

        // VARIABLE TANGGAL UNTUK IMPORT TABEL SECARA DINAMIS
        const latestDataDate = subDays(selectedDate, 2); // - 2 days

        const currMonth = format(latestDataDate, 'MM')
        const currYear = format(latestDataDate, 'yyyy')
        const isPrevMonthLastYear = currMonth === '01'
        const prevMonth = isPrevMonthLastYear ? '12' : format(subMonths(latestDataDate, 1), 'MM')
        const prevMonthYear = isPrevMonthLastYear ? format(subYears(latestDataDate, 1), 'yyyy') : format(latestDataDate, 'yyyy')
        const prevYear = format(subYears(latestDataDate, 1), 'yyyy')

        // TABEL DINAMIS
        const currGrossPrabayarRev = dynamicResumeRevenuePumaTable(currYear, currMonth)
        const prevMonthGrossPrabayarRev = dynamicResumeRevenuePumaTable(prevMonthYear, prevMonth)
        const prevYearCurrMonthGrossPrabayarRev = dynamicResumeRevenuePumaTable(prevYear, currMonth)

        // VARIABLE TANGGAL
        const firstDayOfCurrMonth = format(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 'yyyy-MM-dd')
        const firstDayOfPrevMonth = format(subMonths(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const firstDayOfPrevYearCurrMonth = format(subYears(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const currDate = format(latestDataDate, 'yyyy-MM-dd')
        const prevDate = format(subMonths(latestDataDate, 1), 'yyyy-MM-dd')
        const prevYearCurrDate = format(subYears(latestDataDate, 1), 'yyyy-MM-dd')

        const sq2 = db2
            .select({
                regionName: currGrossPrabayarRev.regionSales,
                branchName: sql<string>`
CASE
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR',
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'BURU',
     'BURU SELATAN',
     'SERAM BAGIAN BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'AMBON'
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'KOTA JAYAPURA',
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'JAYAPURA'
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'MANOKWARI',
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA',
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG'
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'ASMAT',
     'BOVEN DIGOEL',
     'MAPPI',
     'MERAUKE',
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA',
     'DEIYAI',
     'DOGIYAI',
     'NABIRE',
     'PANIAI'
 ) THEN 'TIMIKA'
 ELSE NULL
END
    `.as('branchName'),
                subbranchName: sql<string>`
CASE
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN AMBON'
 WHEN ${currGrossPrabayarRev.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
 WHEN ${currGrossPrabayarRev.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'SENTANI'
 WHEN ${currGrossPrabayarRev.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG RAJA AMPAT'
 WHEN ${currGrossPrabayarRev.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA'
 WHEN ${currGrossPrabayarRev.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 ELSE NULL
END
    `.as('subbranchName'),
                clusterName: sql<string>`
CASE
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN TUAL'
 WHEN ${currGrossPrabayarRev.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
 WHEN ${currGrossPrabayarRev.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
 WHEN ${currGrossPrabayarRev.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN'
 ) THEN 'NEW BIAK NUMFOR'
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'PAPUA PEGUNUNGAN'
 WHEN ${currGrossPrabayarRev.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'NEW SORONG RAJA AMPAT'
 WHEN ${currGrossPrabayarRev.kabupaten} IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA PUNCAK'
 WHEN ${currGrossPrabayarRev.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 WHEN ${currGrossPrabayarRev.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
 ELSE NULL
END
    `.as('clusterName'),
                kabupaten: currGrossPrabayarRev.kabupaten,
                rev: currGrossPrabayarRev.rev,
            })
            .from(currGrossPrabayarRev)
            .where(and(
                inArray(currGrossPrabayarRev.branch, ['AMBON', 'TIMIKA', 'SORONG', 'JAYAPURA']),
                between(currGrossPrabayarRev.mtdDt, firstDayOfCurrMonth, currDate)
            ))
            .as('sq2')

        const sq3 = db2
            .select({
                regionName: prevMonthGrossPrabayarRev.regionSales,
                branchName: sql<string>`
CASE
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR',
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'BURU',
     'BURU SELATAN',
     'SERAM BAGIAN BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'AMBON'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'KOTA JAYAPURA',
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'JAYAPURA'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'MANOKWARI',
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA',
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'ASMAT',
     'BOVEN DIGOEL',
     'MAPPI',
     'MERAUKE',
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA',
     'DEIYAI',
     'DOGIYAI',
     'NABIRE',
     'PANIAI'
 ) THEN 'TIMIKA'
 ELSE NULL
END
    `.as('branchName'),
                subbranchName: sql<string>`
CASE
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN AMBON'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'SENTANI'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG RAJA AMPAT'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 ELSE NULL
END
    `.as('subbranchName'),
                clusterName: sql<string>`
CASE
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN TUAL'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN'
 ) THEN 'NEW BIAK NUMFOR'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'PAPUA PEGUNUNGAN'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'NEW SORONG RAJA AMPAT'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA PUNCAK'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 WHEN ${prevMonthGrossPrabayarRev.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
 ELSE NULL
END
    `.as('clusterName'),
                kabupaten: prevMonthGrossPrabayarRev.kabupaten,
                rev: prevMonthGrossPrabayarRev.rev,
            })
            .from(prevMonthGrossPrabayarRev)
            .where(
                and(
                    inArray(prevMonthGrossPrabayarRev.branch, ['AMBON', 'TIMIKA', 'SORONG', 'JAYAPURA']),
                    between(prevMonthGrossPrabayarRev.mtdDt, firstDayOfPrevMonth, prevDate)
                )
            )
            .as('sq3')

        const sq4 = db2
            .select({
                regionName: sql<string>`CASE WHEN ${prevYearCurrMonthGrossPrabayarRev.regionSales} IN ('PUMA', 'MALUKU DAN PAPUA') THEN 'PUMA' END`.as('regionaName'),
                branchName: sql<string>`
CASE
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR',
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'BURU',
     'BURU SELATAN',
     'SERAM BAGIAN BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'AMBON'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'KOTA JAYAPURA',
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'JAYAPURA'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'MANOKWARI',
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA',
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'ASMAT',
     'BOVEN DIGOEL',
     'MAPPI',
     'MERAUKE',
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA',
     'DEIYAI',
     'DOGIYAI',
     'NABIRE',
     'PANIAI'
 ) THEN 'TIMIKA'
 ELSE NULL
END
    `.as('branchName'),
                subbranchName: sql<string>`
CASE
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN AMBON'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'SENTANI'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG RAJA AMPAT'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 ELSE NULL
END
    `.as('subbranchName'),
                clusterName: sql<string>`
CASE
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN TUAL'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN'
 ) THEN 'NEW BIAK NUMFOR'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'PAPUA PEGUNUNGAN'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'NEW SORONG RAJA AMPAT'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA PUNCAK'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 WHEN ${prevYearCurrMonthGrossPrabayarRev.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
 ELSE NULL
END
    `.as('clusterName'),
                kabupaten: prevYearCurrMonthGrossPrabayarRev.kabupaten,
                rev: prevYearCurrMonthGrossPrabayarRev.rev,
            })
            .from(prevYearCurrMonthGrossPrabayarRev)
            .where(and(
                inArray(prevYearCurrMonthGrossPrabayarRev.branch, ['AMBON', 'TIMIKA', 'SORONG', 'JAYAPURA']),
                between(prevYearCurrMonthGrossPrabayarRev.mtdDt, firstDayOfPrevYearCurrMonth, prevYearCurrDate)
            ))
            .as('sq4')

        // QUERY UNTUK TARGET BULAN INI
        const p1 = db
            .select({
                id: regionals.id,
                region: regionals.regional,
                branch: branches.branchNew,
                subbranch: subbranches.subbranchNew,
                cluster: clusters.cluster,
                kabupaten: kabupatens.kabupaten,
                currMonthTargetRev: sql<number>`CAST(SUM(${revenueGrosses[monthColumn]}) AS DOUBLE PRECISION)`.as('currMonthTargetRev')
            })
            .from(regionals)
            .leftJoin(branches, eq(regionals.id, branches.regionalId))
            .leftJoin(subbranches, eq(branches.id, subbranches.branchId))
            .leftJoin(clusters, eq(subbranches.id, clusters.subbranchId))
            .leftJoin(kabupatens, eq(clusters.id, kabupatens.clusterId))
            .leftJoin(revenueGrosses, eq(kabupatens.id, revenueGrosses.kabupatenId))
            .groupBy(
                regionals.regional,
                branches.branchNew,
                subbranches.subbranchNew,
                clusters.cluster,
                kabupatens.kabupaten
            )
            .orderBy(asc(regionals.regional), asc(branches.branchNew), asc(subbranches.subbranchNew), asc(clusters.cluster), asc(kabupatens.kabupaten))
            .prepare()

        //  QUERY UNTUK MENDAPAT CURRENT MONTH REVENUE (Mtd)
        const p2 = db2
            .select({
                region: sql<string>`${sq2.regionName}`.as('region'),
                branch: sql<string>`${sq2.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq2.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq2.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq2.kabupaten}`.as('kabupaten'),
                currMonthKabupatenRev: sql<number>`SUM(${sq2.rev})`.as('currMonthKabupatenRev'),
                currMonthClusterRev: sql<number>`SUM(SUM(${sq2.rev})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName}, ${sq2.subbranchName}, ${sq2.clusterName})`.as('currMonthClusterRev'),
                currMonthSubbranchRev: sql<number>`SUM(SUM(${sq2.rev})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName}, ${sq2.subbranchName})`.as('currMonthSubbranchRev'),
                currMonthBranchRev: sql<number>`SUM(SUM(${sq2.rev})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName})`.as('currMonthBranchRev'),
                currMonthRegionalRev: sql<number>`SUM(SUM(${sq2.rev})) OVER (PARTITION BY ${sq2.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq2)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        // QUERY UNTUK MENDAPAT PREV MONTH REVENUE
        const p3 = db2
            .select({
                region: sql<string>`${sq3.regionName}`.as('region'),
                branch: sql<string>`${sq3.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq3.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq3.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq3.kabupaten}`.as('kabupaten'),
                prevMonthKabupatenRev: sql<number>`SUM(${sq3.rev})`.as('currMonthKabupatenRev'),
                prevMonthClusterRev: sql<number>`SUM(SUM(${sq3.rev})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName}, ${sq3.subbranchName}, ${sq3.clusterName})`.as('currMonthClusterRev'),
                prevMonthSubbranchRev: sql<number>`SUM(SUM(${sq3.rev})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName}, ${sq3.subbranchName})`.as('currMonthSubbranchRev'),
                prevMonthBranchRev: sql<number>`SUM(SUM(${sq3.rev})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName})`.as('currMonthBranchRev'),
                prevMonthRegionalRev: sql<number>`SUM(SUM(${sq3.rev})) OVER (PARTITION BY ${sq3.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq3)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        // QUERY UNTUK MENDAPAT PREV YEAR CURR MONTH REVENUE
        const p4 = db2
            .select({
                region: sql<string>`${sq4.regionName}`.as('region'),
                branch: sql<string>`${sq4.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq4.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq4.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq4.kabupaten}`.as('kabupaten'),
                prevYearCurrMonthKabupatenRev: sql<number>`SUM(${sq4.rev})`.as('currMonthKabupatenRev'),
                prevYearCurrMonthClusterRev: sql<number>`SUM(SUM(${sq4.rev})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName}, ${sq4.subbranchName}, ${sq4.clusterName})`.as('currMonthClusterRev'),
                prevYearCurrMonthSubbranchRev: sql<number>`SUM(SUM(${sq4.rev})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName}, ${sq4.subbranchName})`.as('currMonthSubbranchRev'),
                prevYearCurrMonthBranchRev: sql<number>`SUM(SUM(${sq4.rev})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName})`.as('currMonthBranchRev'),
                prevYearCurrMonthRegionalRev: sql<number>`SUM(SUM(${sq4.rev})) OVER (PARTITION BY ${sq4.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq4)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        const [targetRevenue, currMonthRevenue, prevMonthRevenue, prevYearCurrMonthRevenue] = await Promise.all([
            p1.execute(),
            p2.execute(),
            p3.execute(),
            p4.execute()
        ]);

        const regionalsMap = new Map();

        targetRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthTarget += Number(row.currMonthTargetRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize kabupaten if it doesn't exist
            cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: Number(row.currMonthTargetRev),
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
        })

        currMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthRevenue = Number(row.currMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.currMonthRevenue = Number(row.currMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.currMonthRevenue = Number(row.currMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.currMonthRevenue = Number(row.currMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));

            kabupaten.currMonthRevenue = Number(row.currMonthKabupatenRev)
        })

        prevMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevMonthRevenue = Number(row.prevMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.prevMonthRevenue = Number(row.prevMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.prevMonthRevenue = Number(row.prevMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevMonthRevenue = Number(row.prevMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
            kabupaten.prevMonthRevenue = Number(row.prevMonthKabupatenRev)
        })

        prevYearCurrMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
            kabupaten.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthKabupatenRev)
        })

        const finalDataRevenue = Array.from(regionalsMap.values()).map((regional: any) => ({
            ...regional,
            branches: Array.from(regional.branches.values()).map((branch: any) => ({
                ...branch,
                subbranches: Array.from(branch.subbranches.values()).map((subbranch: any) => ({
                    ...subbranch,
                    clusters: Array.from(subbranch.clusters.values().map((cluster: any) => ({
                        ...cluster,
                        kabupatens: Array.from(cluster.kabupatens.values())
                    }))),
                })),
            })),
        }));

        return Response.json({ data: finalDataRevenue }, { status: 200 })
    }, {
        query: t.Object({
            date: t.Optional(t.Date())
        })
    })
    .get('/revenue-new-sales', async ({ db, db4, query }) => {
        const { date } = query
        const selectedDate = date ? new Date(date) : new Date()
        const month = (subDays(selectedDate, 2).getMonth() + 1).toString()

        // KOLOM DINAMIS UNTUK MEMILIH ANTARA KOLOM `m1-m12`
        const monthColumn = `m${month}` as keyof typeof revenueNewSales.$inferSelect

        // VARIABLE TANGGAL UNTUK IMPORT TABEL SECARA DINAMIS
        const latestDataDate = subDays(selectedDate, 2);

        const currMonth = format(latestDataDate, 'MM')
        const currYear = format(latestDataDate, 'yyyy')
        const isPrevMonthLastYear = currMonth === '01'
        const prevMonth = isPrevMonthLastYear ? '12' : format(subMonths(latestDataDate, 1), 'MM')
        const prevMonthYear = isPrevMonthLastYear ? format(subYears(latestDataDate, 1), 'yyyy') : format(latestDataDate, 'yyyy')
        const prevYear = format(subYears(latestDataDate, 1), 'yyyy')

        // TABEL `byu_`
        const currRevNewSales = dynamicMergeNewSalesPumaTable(currYear, currMonth)
        const prevMonthRevNewSales = dynamicMergeNewSalesPumaTable(prevMonthYear, prevMonth)
        const prevYearCurrMonthRevNewSales = dynamicMergeNewSalesPumaTable(prevYear, currMonth)

        // VARIABLE TANGGAL
        const firstDayOfCurrMonth = format(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 'yyyy-MM-dd')
        const firstDayOfPrevMonth = format(subMonths(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const firstDayOfPrevYearCurrMonth = format(subYears(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const currDate = format(latestDataDate, 'yyyy-MM-dd')
        const prevDate = format(subMonths(latestDataDate, 1), 'yyyy-MM-dd')
        const prevYearCurrDate = format(subYears(latestDataDate, 1), 'yyyy-MM-dd')

        const sq2 = db4
            .select({
                regionName: currRevNewSales.regionSales,
                branchName: sql<string>`
        CASE
         WHEN ${currRevNewSales.kabupaten} IN (
             'AMBON',
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR',
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'BURU',
             'BURU SELATAN',
             'SERAM BAGIAN BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'AMBON'
         WHEN ${currRevNewSales.kabupaten} IN (
             'KOTA JAYAPURA',
             'JAYAPURA',
             'KEEROM',
             'MAMBERAMO RAYA',
             'SARMI',
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN',
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'JAYAPURA'
         WHEN ${currRevNewSales.kabupaten} IN (
             'MANOKWARI',
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA',
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'SORONG'
         WHEN ${currRevNewSales.kabupaten} IN (
             'ASMAT',
             'BOVEN DIGOEL',
             'MAPPI',
             'MERAUKE',
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA',
             'DEIYAI',
             'DOGIYAI',
             'NABIRE',
             'PANIAI'
         ) THEN 'TIMIKA'
         ELSE NULL
        END
                        `.as('branchName'),
                subbranchName: sql<string>`
        CASE
         WHEN ${currRevNewSales.kabupaten} IN (
             'AMBON',
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR'
         ) THEN 'AMBON'
         WHEN ${currRevNewSales.kabupaten} IN (
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'KEPULAUAN AMBON'
         WHEN ${currRevNewSales.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
         WHEN ${currRevNewSales.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
         WHEN ${currRevNewSales.kabupaten} IN (
             'JAYAPURA',
             'KEEROM',
             'MAMBERAMO RAYA',
             'SARMI',
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN',
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'SENTANI'
         WHEN ${currRevNewSales.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
         WHEN ${currRevNewSales.kabupaten} IN (
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA'
         ) THEN 'MANOKWARI OUTER'
         WHEN ${currRevNewSales.kabupaten} IN (
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'SORONG RAJA AMPAT'
         WHEN ${currRevNewSales.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
         WHEN ${currRevNewSales.kabupaten} IN (
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA'
         ) THEN 'MIMIKA'
         WHEN ${currRevNewSales.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
         ELSE NULL
        END
                        `.as('subbranchName'),
                clusterName: sql<string>`
        CASE
         WHEN ${currRevNewSales.kabupaten} IN (
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR'
         ) THEN 'AMBON'
         WHEN ${currRevNewSales.kabupaten} IN (
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'KEPULAUAN TUAL'
         WHEN ${currRevNewSales.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
         WHEN ${currRevNewSales.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
         WHEN ${currRevNewSales.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
         WHEN ${currRevNewSales.kabupaten} IN (
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN'
         ) THEN 'NEW BIAK NUMFOR'
         WHEN ${currRevNewSales.kabupaten} IN (
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'PAPUA PEGUNUNGAN'
         WHEN ${currRevNewSales.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
         WHEN ${currRevNewSales.kabupaten} IN (
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA'
         ) THEN 'MANOKWARI OUTER'
         WHEN ${currRevNewSales.kabupaten} IN (
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'NEW SORONG RAJA AMPAT'
         WHEN ${currRevNewSales.kabupaten} IN (
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA'
         ) THEN 'MIMIKA PUNCAK'
         WHEN ${currRevNewSales.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
         WHEN ${currRevNewSales.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
         ELSE NULL
        END
                        `.as('clusterName'),
                cityName: currRevNewSales.kabupaten,
                rev: currRevNewSales.rev,
                mtdDt: currRevNewSales.mtdDt
            })
            .from(currRevNewSales)
            .where(and(
                notInArray(currRevNewSales.kabupaten, ['TMP']),
                between(currRevNewSales.mtdDt, firstDayOfCurrMonth, currDate)
            ))
            .as('sq2')

        const sq3 = db4
            .select({
                regionName: prevMonthRevNewSales.regionSales,
                branchName: sql<string>`
        CASE
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'AMBON',
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR',
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'BURU',
             'BURU SELATAN',
             'SERAM BAGIAN BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'AMBON'
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'KOTA JAYAPURA',
             'JAYAPURA',
             'KEEROM',
             'MAMBERAMO RAYA',
             'SARMI',
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN',
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'JAYAPURA'
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'MANOKWARI',
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA',
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'SORONG'
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'ASMAT',
             'BOVEN DIGOEL',
             'MAPPI',
             'MERAUKE',
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA',
             'DEIYAI',
             'DOGIYAI',
             'NABIRE',
             'PANIAI'
         ) THEN 'TIMIKA'
         ELSE NULL
        END
                        `.as('branchName'),
                subbranchName: sql<string>`
        CASE
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'AMBON',
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR'
         ) THEN 'AMBON'
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'KEPULAUAN AMBON'
         WHEN ${prevMonthRevNewSales.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
         WHEN ${prevMonthRevNewSales.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'JAYAPURA',
             'KEEROM',
             'MAMBERAMO RAYA',
             'SARMI',
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN',
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'SENTANI'
         WHEN ${prevMonthRevNewSales.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA'
         ) THEN 'MANOKWARI OUTER'
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'SORONG RAJA AMPAT'
         WHEN ${prevMonthRevNewSales.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA'
         ) THEN 'MIMIKA'
         WHEN ${prevMonthRevNewSales.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
         ELSE NULL
        END
                        `.as('subbranchName'),
                clusterName: sql<string>`
        CASE
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR'
         ) THEN 'AMBON'
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'KEPULAUAN TUAL'
         WHEN ${prevMonthRevNewSales.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
         WHEN ${prevMonthRevNewSales.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
         WHEN ${prevMonthRevNewSales.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN'
         ) THEN 'NEW BIAK NUMFOR'
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'PAPUA PEGUNUNGAN'
         WHEN ${prevMonthRevNewSales.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA'
         ) THEN 'MANOKWARI OUTER'
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'NEW SORONG RAJA AMPAT'
         WHEN ${prevMonthRevNewSales.kabupaten} IN (
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA'
         ) THEN 'MIMIKA PUNCAK'
         WHEN ${prevMonthRevNewSales.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
         WHEN ${prevMonthRevNewSales.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
         ELSE NULL
        END
                        `.as('clusterName'),
                cityName: prevMonthRevNewSales.kabupaten,
                rev: prevMonthRevNewSales.rev,
                mtdDt: prevMonthRevNewSales.mtdDt
            })
            .from(prevMonthRevNewSales)
            .where(and(
                notInArray(prevMonthRevNewSales.kabupaten, ['TMP']),
                between(prevMonthRevNewSales.mtdDt, firstDayOfPrevMonth, prevDate)
            ))
            .as('sq3')

        const sq4 = db4
            .select({
                regionName: prevYearCurrMonthRevNewSales.regionSales,
                branchName: sql<string>`
        CASE
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'AMBON',
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR',
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'BURU',
             'BURU SELATAN',
             'SERAM BAGIAN BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'AMBON'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'KOTA JAYAPURA',
             'JAYAPURA',
             'KEEROM',
             'MAMBERAMO RAYA',
             'SARMI',
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN',
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'JAYAPURA'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'MANOKWARI',
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA',
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'SORONG'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'ASMAT',
             'BOVEN DIGOEL',
             'MAPPI',
             'MERAUKE',
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA',
             'DEIYAI',
             'DOGIYAI',
             'NABIRE',
             'PANIAI'
         ) THEN 'TIMIKA'
         ELSE NULL
        END
                        `.as('branchName'),
                subbranchName: sql<string>`
        CASE
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'AMBON',
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR'
         ) THEN 'AMBON'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'KEPULAUAN AMBON'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'JAYAPURA',
             'KEEROM',
             'MAMBERAMO RAYA',
             'SARMI',
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN',
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'SENTANI'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA'
         ) THEN 'MANOKWARI OUTER'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'SORONG RAJA AMPAT'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA'
         ) THEN 'MIMIKA'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
         ELSE NULL
        END
                        `.as('subbranchName'),
                clusterName: sql<string>`
        CASE
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR'
         ) THEN 'AMBON'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'KEPULAUAN TUAL'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN'
         ) THEN 'NEW BIAK NUMFOR'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'PAPUA PEGUNUNGAN'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA'
         ) THEN 'MANOKWARI OUTER'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'NEW SORONG RAJA AMPAT'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN (
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA'
         ) THEN 'MIMIKA PUNCAK'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
         WHEN ${prevYearCurrMonthRevNewSales.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
         ELSE NULL
        END
                        `.as('clusterName'),
                cityName: prevYearCurrMonthRevNewSales.kabupaten,
                rev: prevYearCurrMonthRevNewSales.rev,
                mtdDt: prevYearCurrMonthRevNewSales.mtdDt
            })
            .from(prevYearCurrMonthRevNewSales)
            .where(and(
                notInArray(prevYearCurrMonthRevNewSales.kabupaten, ['TMP']),
                between(prevYearCurrMonthRevNewSales.mtdDt, firstDayOfPrevYearCurrMonth, prevYearCurrDate)
            ))
            .as('sq4')

        // QUERY UNTUK TARGET BULAN INI
        const p1 = db
            .select({
                id: regionals.id,
                region: regionals.regional,
                branch: branches.branchNew,
                subbranch: subbranches.subbranchNew,
                cluster: clusters.cluster,
                kabupaten: kabupatens.kabupaten,
                currMonthTargetRev: sql<number>`CAST(SUM(${revenueNewSales[monthColumn]}) AS DOUBLE PRECISION)`.as('currMonthTargetRev')
            })
            .from(regionals)
            .leftJoin(branches, eq(regionals.id, branches.regionalId))
            .leftJoin(subbranches, eq(branches.id, subbranches.branchId))
            .leftJoin(clusters, eq(subbranches.id, clusters.subbranchId))
            .leftJoin(kabupatens, eq(clusters.id, kabupatens.clusterId))
            .leftJoin(revenueNewSales, eq(kabupatens.id, revenueNewSales.kabupatenId))
            .groupBy(
                regionals.regional,
                branches.branchNew,
                subbranches.subbranchNew,
                clusters.cluster,
                kabupatens.kabupaten
            )
            .orderBy(asc(regionals.regional))
            .prepare()

        //  QUERY UNTUK MENDAPAT CURRENT MONTH REVENUE (Mtd)
        const p2 = db4
            .select({
                region: sql<string>`${sq2.regionName}`.as('region'),
                branch: sql<string>`${sq2.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq2.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq2.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq2.cityName}`.as('kabupaten'),
                currMonthKabupatenRev: sql<number>`SUM(${sq2.rev})`.as('currMonthKabupatenRev'),
                currMonthClusterRev: sql<number>`SUM(SUM(${sq2.rev})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName}, ${sq2.subbranchName}, ${sq2.clusterName})`.as('currMonthClusterRev'),
                currMonthSubbranchRev: sql<number>`SUM(SUM(${sq2.rev})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName}, ${sq2.subbranchName})`.as('currMonthSubbranchRev'),
                currMonthBranchRev: sql<number>`SUM(SUM(${sq2.rev})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName})`.as('currMonthBranchRev'),
                currMonthRegionalRev: sql<number>`SUM(SUM(${sq2.rev})) OVER (PARTITION BY ${sq2.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq2)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        // QUERY UNTUK MENDAPAT PREV MONTH REVENUE
        const p3 = db4
            .select({
                region: sql<string>`${sq3.regionName}`.as('region'),
                branch: sql<string>`${sq3.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq3.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq3.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq3.cityName}`.as('kabupaten'),
                prevMonthKabupatenRev: sql<number>`SUM(${sq3.rev})`.as('currMonthKabupatenRev'),
                prevMonthClusterRev: sql<number>`SUM(SUM(${sq3.rev})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName}, ${sq3.subbranchName}, ${sq3.clusterName})`.as('currMonthClusterRev'),
                prevMonthSubbranchRev: sql<number>`SUM(SUM(${sq3.rev})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName}, ${sq3.subbranchName})`.as('currMonthSubbranchRev'),
                prevMonthBranchRev: sql<number>`SUM(SUM(${sq3.rev})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName})`.as('currMonthBranchRev'),
                prevMonthRegionalRev: sql<number>`SUM(SUM(${sq3.rev})) OVER (PARTITION BY ${sq3.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq3)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        // QUERY UNTUK MENDAPAT PREV YEAR CURR MONTH REVENUE
        const p4 = db4
            .select({
                region: sql<string>`${sq4.regionName}`.as('region'),
                branch: sql<string>`${sq4.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq4.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq4.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq4.cityName}`.as('kabupaten'),
                prevYearCurrMonthKabupatenRev: sql<number>`SUM(${sq4.rev})`.as('currMonthKabupatenRev'),
                prevYearCurrMonthClusterRev: sql<number>`SUM(SUM(${sq4.rev})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName}, ${sq4.subbranchName}, ${sq4.clusterName})`.as('currMonthClusterRev'),
                prevYearCurrMonthSubbranchRev: sql<number>`SUM(SUM(${sq4.rev})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName}, ${sq4.subbranchName})`.as('currMonthSubbranchRev'),
                prevYearCurrMonthBranchRev: sql<number>`SUM(SUM(${sq4.rev})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName})`.as('currMonthBranchRev'),
                prevYearCurrMonthRegionalRev: sql<number>`SUM(SUM(${sq4.rev})) OVER (PARTITION BY ${sq4.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq4)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        // QUERY UNTUK YtD 2025

        const [targetRevenue, currMonthRevenue, prevMonthRevenue, prevYearCurrMonthRevenue] = await Promise.all([
            p1.execute(),
            p2.execute(),
            p3.execute(),
            p4.execute()
        ])
        // /var/lib/backup_mysql_2025/
        const regionalsMap = new Map();

        targetRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthTarget += Number(row.currMonthTargetRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize kabupaten if it doesn't exist
            cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: Number(row.currMonthTargetRev),
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
        })

        currMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthRevenue = Number(row.currMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.currMonthRevenue = Number(row.currMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.currMonthRevenue = Number(row.currMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.currMonthRevenue = Number(row.currMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));

            kabupaten.currMonthRevenue = Number(row.currMonthKabupatenRev)
        })

        prevMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevMonthRevenue = Number(row.prevMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.prevMonthRevenue = Number(row.prevMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.prevMonthRevenue = Number(row.prevMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevMonthRevenue = Number(row.prevMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
            kabupaten.prevMonthRevenue = Number(row.prevMonthKabupatenRev)
        })

        prevYearCurrMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
            kabupaten.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthKabupatenRev)
        })

        const finalDataRevenue = Array.from(regionalsMap.values()).map((regional: any) => ({
            ...regional,
            branches: Array.from(regional.branches.values()).map((branch: any) => ({
                ...branch,
                subbranches: Array.from(branch.subbranches.values()).map((subbranch: any) => ({
                    ...subbranch,
                    clusters: Array.from(subbranch.clusters.values().map((cluster: any) => ({
                        ...cluster,
                        kabupatens: Array.from(cluster.kabupatens.values())
                    }))),
                })),
            })),
        }));

        return Response.json({ data: finalDataRevenue }, { status: 200 })
    }, {
        query: t.Object({
            date: t.Optional(t.Date())
        })
    })
    .get('/revenue-cvm', async ({ db, db2, query }) => {
        const { date } = query
        const selectedDate = date ? new Date(date) : new Date()
        const month = (subDays(selectedDate, 3).getMonth() + 1).toString()

        // KOLOM DINAMIS UNTUK MEMILIH ANTARA KOLOM `m1-m12`
        const monthColumn = `m${month}` as keyof typeof revenueCVM.$inferSelect

        // VARIABLE TANGGAL UNTUK IMPORT TABEL SECARA DINAMIS
        const latestDataDate = subDays(selectedDate, 3);

        const currMonth = format(latestDataDate, 'MM')
        const currYear = format(latestDataDate, 'yyyy')
        const isPrevMonthLastYear = currMonth === '01'
        const prevMonth = isPrevMonthLastYear ? '12' : format(subMonths(latestDataDate, 1), 'MM')
        const prevMonthYear = isPrevMonthLastYear ? format(subYears(latestDataDate, 1), 'yyyy') : format(latestDataDate, 'yyyy')
        const prevYear = format(subYears(latestDataDate, 1), 'yyyy')

        // TABEL `bba_broadband_daily_`
        const currRevCVM = dynamicRevenueCVMTable(currYear, currMonth)
        const prevMonthRevCVM = dynamicRevenueCVMTable(prevMonthYear, prevMonth)
        const prevYearCurrMonthRevCVM = dynamicRevenueCVMTable(prevYear, currMonth)

        // VARIABLE TANGGAL
        const firstDayOfCurrMonth = format(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 'yyyy-MM-dd')
        const firstDayOfPrevMonth = format(subMonths(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const firstDayOfPrevYearCurrMonth = format(subYears(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const currDate = format(latestDataDate, 'yyyy-MM-dd')
        const prevDate = format(subMonths(latestDataDate, 1), 'yyyy-MM-dd')
        const prevYearCurrDate = format(subYears(latestDataDate, 1), 'yyyy-MM-dd')

        const sq2 = db2
            .select({
                regionName: currRevCVM.region,
                branchName: sql<string>`
CASE
 WHEN ${currRevCVM.city} IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR',
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'BURU',
     'BURU SELATAN',
     'SERAM BAGIAN BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'AMBON'
 WHEN ${currRevCVM.city} IN (
     'KOTA JAYAPURA',
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'JAYAPURA'
 WHEN ${currRevCVM.city} IN (
     'MANOKWARI',
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA',
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG'
 WHEN ${currRevCVM.city} IN (
     'ASMAT',
     'BOVEN DIGOEL',
     'MAPPI',
     'MERAUKE',
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA',
     'DEIYAI',
     'DOGIYAI',
     'NABIRE',
     'PANIAI'
 ) THEN 'TIMIKA'
 ELSE NULL
END
                `.as('branchName'),
                subbranchName: sql<string>`
CASE
 WHEN ${currRevCVM.city} IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN ${currRevCVM.city} IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN AMBON'
 WHEN ${currRevCVM.city} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
 WHEN ${currRevCVM.city} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
 WHEN ${currRevCVM.city} IN (
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'SENTANI'
 WHEN ${currRevCVM.city} IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN ${currRevCVM.city} IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN ${currRevCVM.city} IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG RAJA AMPAT'
 WHEN ${currRevCVM.city} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
 WHEN ${currRevCVM.city} IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA'
 WHEN ${currRevCVM.city} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 ELSE NULL
END
                `.as('subbranchName'),
                clusterName: sql<string>`
CASE
 WHEN ${currRevCVM.city} IN (
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN ${currRevCVM.city} IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN TUAL'
 WHEN ${currRevCVM.city} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
 WHEN ${currRevCVM.city} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
 WHEN ${currRevCVM.city} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
 WHEN ${currRevCVM.city} IN (
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN'
 ) THEN 'NEW BIAK NUMFOR'
 WHEN ${currRevCVM.city} IN (
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'PAPUA PEGUNUNGAN'
 WHEN ${currRevCVM.city} IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN ${currRevCVM.city} IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN ${currRevCVM.city} IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'NEW SORONG RAJA AMPAT'
 WHEN ${currRevCVM.city} IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA PUNCAK'
 WHEN ${currRevCVM.city} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 WHEN ${currRevCVM.city} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
 ELSE NULL
END
                `.as('clusterName'),
                cityName: currRevCVM.city,
                rev: currRevCVM.revenue,
                trxDate: currRevCVM.trxDate
            })
            .from(currRevCVM)
            .where(and(
                notInArray(currRevCVM.city, ['TMP']),
                and(
                    like(currRevCVM.packageGroup, '%CVM%'),
                    between(currRevCVM.trxDate, firstDayOfCurrMonth, currDate)
                )
            ))
            .as('sq2')

        const sq3 = db2
            .select({
                regionName: prevMonthRevCVM.region,
                branchName: sql<string>`
CASE
 WHEN ${prevMonthRevCVM.city} IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR',
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'BURU',
     'BURU SELATAN',
     'SERAM BAGIAN BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'AMBON'
 WHEN ${prevMonthRevCVM.city} IN (
     'KOTA JAYAPURA',
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'JAYAPURA'
 WHEN ${prevMonthRevCVM.city} IN (
     'MANOKWARI',
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA',
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG'
 WHEN ${prevMonthRevCVM.city} IN (
     'ASMAT',
     'BOVEN DIGOEL',
     'MAPPI',
     'MERAUKE',
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA',
     'DEIYAI',
     'DOGIYAI',
     'NABIRE',
     'PANIAI'
 ) THEN 'TIMIKA'
 ELSE NULL
END
                `.as('branchName'),
                subbranchName: sql<string>`
CASE
 WHEN ${prevMonthRevCVM.city} IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN ${prevMonthRevCVM.city} IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN AMBON'
 WHEN ${prevMonthRevCVM.city} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
 WHEN ${prevMonthRevCVM.city} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
 WHEN ${prevMonthRevCVM.city} IN (
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'SENTANI'
 WHEN ${prevMonthRevCVM.city} IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN ${prevMonthRevCVM.city} IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN ${prevMonthRevCVM.city} IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG RAJA AMPAT'
 WHEN ${prevMonthRevCVM.city} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
 WHEN ${prevMonthRevCVM.city} IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA'
 WHEN ${prevMonthRevCVM.city} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 ELSE NULL
END
                `.as('subbranchName'),
                clusterName: sql<string>`
CASE
 WHEN ${prevMonthRevCVM.city} IN (
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN ${prevMonthRevCVM.city} IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN TUAL'
 WHEN ${prevMonthRevCVM.city} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
 WHEN ${prevMonthRevCVM.city} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
 WHEN ${prevMonthRevCVM.city} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
 WHEN ${prevMonthRevCVM.city} IN (
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN'
 ) THEN 'NEW BIAK NUMFOR'
 WHEN ${prevMonthRevCVM.city} IN (
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'PAPUA PEGUNUNGAN'
 WHEN ${prevMonthRevCVM.city} IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN ${prevMonthRevCVM.city} IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN ${prevMonthRevCVM.city} IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'NEW SORONG RAJA AMPAT'
 WHEN ${prevMonthRevCVM.city} IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA PUNCAK'
 WHEN ${prevMonthRevCVM.city} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 WHEN ${prevMonthRevCVM.city} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
 ELSE NULL
END
                `.as('clusterName'),
                cityName: prevMonthRevCVM.city,
                rev: prevMonthRevCVM.revenue,
                trxDate: prevMonthRevCVM.trxDate
            })
            .from(prevMonthRevCVM)
            .where(and(
                notInArray(prevMonthRevCVM.city, ['TMP']),
                and(
                    like(prevMonthRevCVM.packageGroup, '%CVM%'),
                    between(prevMonthRevCVM.trxDate, firstDayOfPrevMonth, prevDate)
                )
            ))
            .as('sq3')

        const sq4 = db2
            .select({
                regionName: prevYearCurrMonthRevCVM.region,
                branchName: sql<string>`
CASE
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR',
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'BURU',
     'BURU SELATAN',
     'SERAM BAGIAN BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'AMBON'
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'KOTA JAYAPURA',
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'JAYAPURA'
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'MANOKWARI',
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA',
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG'
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'ASMAT',
     'BOVEN DIGOEL',
     'MAPPI',
     'MERAUKE',
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA',
     'DEIYAI',
     'DOGIYAI',
     'NABIRE',
     'PANIAI'
 ) THEN 'TIMIKA'
 ELSE NULL
END
                `.as('branchName'),
                subbranchName: sql<string>`
CASE
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN AMBON'
 WHEN ${prevYearCurrMonthRevCVM.city} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
 WHEN ${prevYearCurrMonthRevCVM.city} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'SENTANI'
 WHEN ${prevYearCurrMonthRevCVM.city} IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG RAJA AMPAT'
 WHEN ${prevYearCurrMonthRevCVM.city} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA'
 WHEN ${prevYearCurrMonthRevCVM.city} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 ELSE NULL
END
                `.as('subbranchName'),
                clusterName: sql<string>`
CASE
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN TUAL'
 WHEN ${prevYearCurrMonthRevCVM.city} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
 WHEN ${prevYearCurrMonthRevCVM.city} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
 WHEN ${prevYearCurrMonthRevCVM.city} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN'
 ) THEN 'NEW BIAK NUMFOR'
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'PAPUA PEGUNUNGAN'
 WHEN ${prevYearCurrMonthRevCVM.city} IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'NEW SORONG RAJA AMPAT'
 WHEN ${prevYearCurrMonthRevCVM.city} IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA PUNCAK'
 WHEN ${prevYearCurrMonthRevCVM.city} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 WHEN ${prevYearCurrMonthRevCVM.city} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
 ELSE NULL
END
                `.as('clusterName'),
                cityName: prevYearCurrMonthRevCVM.city,
                rev: prevYearCurrMonthRevCVM.revenue,
                trxDate: prevYearCurrMonthRevCVM.trxDate
            })
            .from(prevYearCurrMonthRevCVM)
            .where(and(
                notInArray(prevYearCurrMonthRevCVM.city, ['TMP']),
                and(
                    like(prevYearCurrMonthRevCVM.packageGroup, '%CVM%'),
                    between(prevYearCurrMonthRevCVM.trxDate, firstDayOfPrevYearCurrMonth, prevYearCurrDate)
                )
            ))
            .as('sq4')

        // QUERY UNTUK TARGET BULAN INI
        const p1 = db
            .select({
                id: regionals.id,
                region: regionals.regional,
                branch: branches.branchNew,
                subbranch: subbranches.subbranchNew,
                cluster: clusters.cluster,
                kabupaten: kabupatens.kabupaten,
                currMonthTargetRev: sql<number>`CAST(SUM(${revenueCVM[monthColumn]}) AS DOUBLE PRECISION)`.as('currMonthTargetRev')
            })
            .from(regionals)
            .leftJoin(branches, eq(regionals.id, branches.regionalId))
            .leftJoin(subbranches, eq(branches.id, subbranches.branchId))
            .leftJoin(clusters, eq(subbranches.id, clusters.subbranchId))
            .leftJoin(kabupatens, eq(clusters.id, kabupatens.clusterId))
            .leftJoin(revenueCVM, eq(kabupatens.id, revenueCVM.kabupatenId))
            .groupBy(
                regionals.regional,
                branches.branchNew,
                subbranches.subbranchNew,
                clusters.cluster,
                kabupatens.kabupaten
            )
            .orderBy(asc(regionals.regional), asc(branches.branchNew), asc(subbranches.subbranchNew), asc(clusters.cluster), asc(kabupatens.kabupaten))
            .prepare()

        //  QUERY UNTUK MENDAPAT CURRENT MONTH REVENUE (Mtd)
        const p2 = db2
            .select({
                region: sql<string>`${sq2.regionName}`.as('region'),
                branch: sql<string>`${sq2.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq2.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq2.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq2.cityName}`.as('kabupaten'),
                currMonthKabupatenRev: sql<number>`SUM(${sq2.rev})`.as('currMonthKabupatenRev'),
                currMonthClusterRev: sql<number>`SUM(SUM(${sq2.rev})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName}, ${sq2.subbranchName}, ${sq2.clusterName})`.as('currMonthClusterRev'),
                currMonthSubbranchRev: sql<number>`SUM(SUM(${sq2.rev})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName}, ${sq2.subbranchName})`.as('currMonthSubbranchRev'),
                currMonthBranchRev: sql<number>`SUM(SUM(${sq2.rev})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName})`.as('currMonthBranchRev'),
                currMonthRegionalRev: sql<number>`SUM(SUM(${sq2.rev})) OVER (PARTITION BY ${sq2.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq2)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        // QUERY UNTUK MENDAPAT PREV MONTH REVENUE
        const p3 = db2
            .select({
                region: sql<string>`${sq3.regionName}`.as('region'),
                branch: sql<string>`${sq3.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq3.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq3.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq3.cityName}`.as('kabupaten'),
                prevMonthKabupatenRev: sql<number>`SUM(${sq3.rev})`.as('currMonthKabupatenRev'),
                prevMonthClusterRev: sql<number>`SUM(SUM(${sq3.rev})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName}, ${sq3.subbranchName}, ${sq3.clusterName})`.as('currMonthClusterRev'),
                prevMonthSubbranchRev: sql<number>`SUM(SUM(${sq3.rev})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName}, ${sq3.subbranchName})`.as('currMonthSubbranchRev'),
                prevMonthBranchRev: sql<number>`SUM(SUM(${sq3.rev})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName})`.as('currMonthBranchRev'),
                prevMonthRegionalRev: sql<number>`SUM(SUM(${sq3.rev})) OVER (PARTITION BY ${sq3.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq3)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        // QUERY UNTUK MENDAPAT PREV YEAR CURR MONTH REVENUE
        const p4 = db2
            .select({
                region: sql<string>`${sq4.regionName}`.as('region'),
                branch: sql<string>`${sq4.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq4.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq4.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq4.cityName}`.as('kabupaten'),
                prevYearCurrMonthKabupatenRev: sql<number>`SUM(${sq4.rev})`.as('currMonthKabupatenRev'),
                prevYearCurrMonthClusterRev: sql<number>`SUM(SUM(${sq4.rev})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName}, ${sq4.subbranchName}, ${sq4.clusterName})`.as('currMonthClusterRev'),
                prevYearCurrMonthSubbranchRev: sql<number>`SUM(SUM(${sq4.rev})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName}, ${sq4.subbranchName})`.as('currMonthSubbranchRev'),
                prevYearCurrMonthBranchRev: sql<number>`SUM(SUM(${sq4.rev})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName})`.as('currMonthBranchRev'),
                prevYearCurrMonthRegionalRev: sql<number>`SUM(SUM(${sq4.rev})) OVER (PARTITION BY ${sq4.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq4)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        // QUERY UNTUK YtD 2025

        const [targetRevenue, currMonthRevenue, prevMonthRevenue, prevYearCurrMonthRevenue] = await Promise.all([
            p1.execute(),
            p2.execute(),
            p3.execute(),
            p4.execute()
        ])
        // /var/lib/backup_mysql_2025/
        const regionalsMap = new Map();

        targetRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthTarget += Number(row.currMonthTargetRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize kabupaten if it doesn't exist
            cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: Number(row.currMonthTargetRev),
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
        })

        currMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthRevenue = Number(row.currMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.currMonthRevenue = Number(row.currMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.currMonthRevenue = Number(row.currMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.currMonthRevenue = Number(row.currMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));

            kabupaten.currMonthRevenue = Number(row.currMonthKabupatenRev)
        })

        prevMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevMonthRevenue = Number(row.prevMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.prevMonthRevenue = Number(row.prevMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.prevMonthRevenue = Number(row.prevMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevMonthRevenue = Number(row.prevMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
            kabupaten.prevMonthRevenue = Number(row.prevMonthKabupatenRev)
        })

        prevYearCurrMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
            kabupaten.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthKabupatenRev)
        })

        const finalDataRevenue = Array.from(regionalsMap.values()).map((regional: any) => ({
            ...regional,
            branches: Array.from(regional.branches.values()).map((branch: any) => ({
                ...branch,
                subbranches: Array.from(branch.subbranches.values()).map((subbranch: any) => ({
                    ...subbranch,
                    clusters: Array.from(subbranch.clusters.values().map((cluster: any) => ({
                        ...cluster,
                        kabupatens: Array.from(cluster.kabupatens.values())
                    }))),
                })),
            })),
        }));

        return Response.json({ data: finalDataRevenue }, { status: 200 })
    }, {
        query: t.Object({
            date: t.Optional(t.Date())
        })
    })
    .get('/revenue-sa', async ({ db, db5, query }) => {
        const { date } = query
        const selectedDate = date ? new Date(date) : new Date()
        const month = (subDays(selectedDate, 3).getMonth() + 1).toString()

        // KOLOM DINAMIS UNTUK MEMILIH ANTARA KOLOM `m1-m12`
        const monthColumn = `m${month}` as keyof typeof revenueSA.$inferSelect

        // VARIABLE TANGGAL UNTUK IMPORT TABEL SECARA DINAMIS
        const latestDataDate = subDays(selectedDate, 3); // - 3 days

        const currMonth = format(latestDataDate, 'MM')
        const currYear = format(latestDataDate, 'yyyy')
        const isPrevMonthLastYear = currMonth === '01'
        const prevMonth = isPrevMonthLastYear ? '12' : format(subMonths(latestDataDate, 1), 'MM')
        const prevMonthYear = isPrevMonthLastYear ? format(subYears(latestDataDate, 1), 'yyyy') : format(latestDataDate, 'yyyy')
        const prevYear = format(subYears(latestDataDate, 1), 'yyyy')

        // TABEL `sa_detil_`
        const currRevSA = dynamicRevenueSATable(currYear, currMonth)
        const prevMonthRevSA = dynamicRevenueSATable(prevMonthYear, prevMonth)
        const prevYearCurrMonthRevSA = dynamicRevenueSATable(prevYear, currMonth)

        // VARIABLE TANGGAL
        const firstDayOfCurrMonth = format(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 'yyyy-MM-dd')
        const firstDayOfPrevMonth = format(subMonths(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const firstDayOfPrevYearCurrMonth = format(subYears(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const currDate = format(latestDataDate, 'yyyy-MM-dd')
        const prevDate = format(subMonths(latestDataDate, 1), 'yyyy-MM-dd')
        const prevYearCurrDate = format(subYears(latestDataDate, 1), 'yyyy-MM-dd')

        const sq2 = db5
            .select({
                regionName: sql<string>`CASE WHEN ${currRevSA.regional} IN ('MALUKU DAN PAPUA', 'PUMA') THEN 'PUMA' END`.as('regionName'),
                branchName: sql<string>`
     CASE
         WHEN ${currRevSA.kabupaten} IN (
             'AMBON',
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR',
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'BURU',
             'BURU SELATAN',
             'SERAM BAGIAN BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'AMBON'
         WHEN ${currRevSA.kabupaten} IN (
             'KOTA JAYAPURA',
             'JAYAPURA',
             'KEEROM',
             'MAMBERAMO RAYA',
             'SARMI',
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN',
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'JAYAPURA'
         WHEN ${currRevSA.kabupaten} IN (
             'MANOKWARI',
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA',
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'SORONG'
         WHEN ${currRevSA.kabupaten} IN (
             'ASMAT',
             'BOVEN DIGOEL',
             'MAPPI',
             'MERAUKE',
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA',
             'DEIYAI',
             'DOGIYAI',
             'NABIRE',
             'PANIAI'
         ) THEN 'TIMIKA'
         ELSE NULL
     END
            `.as('branchName'),
                subbranchName: sql<string>`
     CASE
         WHEN ${currRevSA.kabupaten} IN (
             'AMBON',
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR'
         ) THEN 'AMBON'
         WHEN ${currRevSA.kabupaten} IN (
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'KEPULAUAN AMBON'
         WHEN ${currRevSA.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
         WHEN ${currRevSA.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
         WHEN ${currRevSA.kabupaten} IN (
             'JAYAPURA',
             'KEEROM',
             'MAMBERAMO RAYA',
             'SARMI',
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN',
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'SENTANI'
         WHEN ${currRevSA.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
         WHEN ${currRevSA.kabupaten} IN (
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA'
         ) THEN 'MANOKWARI OUTER'
         WHEN ${currRevSA.kabupaten} IN (
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'SORONG RAJA AMPAT'
         WHEN ${currRevSA.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
         WHEN ${currRevSA.kabupaten} IN (
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA'
         ) THEN 'MIMIKA'
         WHEN ${currRevSA.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
         ELSE NULL
     END
            `.as('subbranchName'),
                clusterName: sql<string>`
     CASE
         WHEN ${currRevSA.kabupaten} IN (
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR'
         ) THEN 'AMBON'
         WHEN ${currRevSA.kabupaten} IN (
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'KEPULAUAN TUAL'
         WHEN ${currRevSA.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
         WHEN ${currRevSA.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
         WHEN ${currRevSA.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
         WHEN ${currRevSA.kabupaten} IN (
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN'
         ) THEN 'NEW BIAK NUMFOR'
         WHEN ${currRevSA.kabupaten} IN (
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'PAPUA PEGUNUNGAN'
         WHEN ${currRevSA.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
         WHEN ${currRevSA.kabupaten} IN (
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA'
         ) THEN 'MANOKWARI OUTER'
         WHEN ${currRevSA.kabupaten} IN (
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'NEW SORONG RAJA AMPAT'
         WHEN ${currRevSA.kabupaten} IN (
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA'
         ) THEN 'MIMIKA PUNCAK'
         WHEN ${currRevSA.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
         WHEN ${currRevSA.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
         ELSE NULL
     END
            `.as('clusterName'),
                kabupaten: currRevSA.kabupaten,
                price: currRevSA.price,
                trx: sql<number>`COUNT(${currRevSA.msisdn})`.as('trx')
            })
            .from(currRevSA)
            .where(and(
                not(eq(currRevSA.kabupaten, 'TMP')),
                and(
                    inArray(currRevSA.regional, ['MALUKU DAN PAPUA', 'PUMA']),
                    between(currRevSA.trxDate, firstDayOfCurrMonth, currDate)
                )
            ))
            .groupBy(sql`1,2,3,4,5`)
            .as('sq2')

        const regClassP2 = db5.select({
            regionName: sq2.regionName,
            branchName: sq2.branchName,
            subbranchName: sq2.subbranchName,
            clusterName: sq2.clusterName,
            cityName: sq2.kabupaten,
            revenue: sql<number>`SUM(${sq2.price} * ${sq2.trx})`.as('revenue')
        })
            .from(sq2)
            .groupBy(sql`1,2,3,4,5`)
            .as('regionClassififcation')

        const sq3 = db5
            .select({
                regionName: sql<string>`CASE WHEN ${prevMonthRevSA.regional} IN ('MALUKU DAN PAPUA', 'PUMA') THEN 'PUMA' END`.as('regionName'),
                branchName: sql<string>`
     CASE
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'AMBON',
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR',
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'BURU',
             'BURU SELATAN',
             'SERAM BAGIAN BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'AMBON'
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'KOTA JAYAPURA',
             'JAYAPURA',
             'KEEROM',
             'MAMBERAMO RAYA',
             'SARMI',
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN',
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'JAYAPURA'
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'MANOKWARI',
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA',
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'SORONG'
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'ASMAT',
             'BOVEN DIGOEL',
             'MAPPI',
             'MERAUKE',
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA',
             'DEIYAI',
             'DOGIYAI',
             'NABIRE',
             'PANIAI'
         ) THEN 'TIMIKA'
         ELSE NULL
     END
            `.as('branchName'),
                subbranchName: sql<string>`
     CASE
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'AMBON',
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR'
         ) THEN 'AMBON'
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'KEPULAUAN AMBON'
         WHEN ${prevMonthRevSA.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
         WHEN ${prevMonthRevSA.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'JAYAPURA',
             'KEEROM',
             'MAMBERAMO RAYA',
             'SARMI',
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN',
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'SENTANI'
         WHEN ${prevMonthRevSA.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA'
         ) THEN 'MANOKWARI OUTER'
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'SORONG RAJA AMPAT'
         WHEN ${prevMonthRevSA.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA'
         ) THEN 'MIMIKA'
         WHEN ${prevMonthRevSA.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
         ELSE NULL
     END
            `.as('subbranchName'),
                clusterName: sql<string>`
     CASE
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR'
         ) THEN 'AMBON'
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'KEPULAUAN TUAL'
         WHEN ${prevMonthRevSA.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
         WHEN ${prevMonthRevSA.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
         WHEN ${prevMonthRevSA.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN'
         ) THEN 'NEW BIAK NUMFOR'
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'PAPUA PEGUNUNGAN'
         WHEN ${prevMonthRevSA.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA'
         ) THEN 'MANOKWARI OUTER'
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'NEW SORONG RAJA AMPAT'
         WHEN ${prevMonthRevSA.kabupaten} IN (
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA'
         ) THEN 'MIMIKA PUNCAK'
         WHEN ${prevMonthRevSA.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
         WHEN ${prevMonthRevSA.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
         ELSE NULL
     END
            `.as('clusterName'),
                kabupaten: prevMonthRevSA.kabupaten,
                price: prevMonthRevSA.price,
                trx: sql<number>`COUNT(${prevMonthRevSA.msisdn})`.as('trx')
            })
            .from(prevMonthRevSA)
            .where(and(
                not(eq(prevMonthRevSA.kabupaten, 'TMP')),
                and(
                    inArray(prevMonthRevSA.regional, ['MALUKU DAN PAPUA', 'PUMA']),
                    between(prevMonthRevSA.trxDate, firstDayOfPrevMonth, prevDate)
                )
            ))
            .groupBy(sql`1,2,3,4,5`)
            .as('sq2')

        const regClassP3 = db5.select({
            regionName: sq3.regionName,
            branchName: sq3.branchName,
            subbranchName: sq3.subbranchName,
            clusterName: sq3.clusterName,
            cityName: sq3.kabupaten,
            revenue: sql<number>`SUM(${sq3.price} * ${sq3.trx})`.as('revenue')
        })
            .from(sq3)
            .groupBy(sql`1,2,3,4,5`)
            .as('regionClassififcation')

        const sq4 = db5
            .select({
                regionName: sql<string>`CASE WHEN ${prevYearCurrMonthRevSA.regional} IN ('MALUKU DAN PAPUA', 'PUMA') THEN 'PUMA' END`.as('regionName'),
                branchName: sql<string>`
     CASE
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'AMBON',
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR',
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'BURU',
             'BURU SELATAN',
             'SERAM BAGIAN BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'AMBON'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'KOTA JAYAPURA',
             'JAYAPURA',
             'KEEROM',
             'MAMBERAMO RAYA',
             'SARMI',
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN',
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'JAYAPURA'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'MANOKWARI',
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA',
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'SORONG'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'ASMAT',
             'BOVEN DIGOEL',
             'MAPPI',
             'MERAUKE',
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA',
             'DEIYAI',
             'DOGIYAI',
             'NABIRE',
             'PANIAI'
         ) THEN 'TIMIKA'
         ELSE NULL
     END
            `.as('branchName'),
                subbranchName: sql<string>`
     CASE
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'AMBON',
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR'
         ) THEN 'AMBON'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'KEPULAUAN AMBON'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'JAYAPURA',
             'KEEROM',
             'MAMBERAMO RAYA',
             'SARMI',
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN',
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'SENTANI'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA'
         ) THEN 'MANOKWARI OUTER'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'SORONG RAJA AMPAT'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA'
         ) THEN 'MIMIKA'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
         ELSE NULL
     END
            `.as('subbranchName'),
                clusterName: sql<string>`
     CASE
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'KOTA AMBON',
             'MALUKU TENGAH',
             'SERAM BAGIAN TIMUR'
         ) THEN 'AMBON'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'KEPULAUAN ARU',
             'KOTA TUAL',
             'MALUKU BARAT DAYA',
             'MALUKU TENGGARA',
             'MALUKU TENGGARA BARAT',
             'KEPULAUAN TANIMBAR'
         ) THEN 'KEPULAUAN TUAL'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'BIAK',
             'BIAK NUMFOR',
             'KEPULAUAN YAPEN',
             'SUPIORI',
             'WAROPEN'
         ) THEN 'NEW BIAK NUMFOR'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'JAYAWIJAYA',
             'LANNY JAYA',
             'MAMBERAMO TENGAH',
             'NDUGA',
             'PEGUNUNGAN BINTANG',
             'TOLIKARA',
             'YAHUKIMO',
             'YALIMO'
         ) THEN 'PAPUA PEGUNUNGAN'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'FAKFAK',
             'FAK FAK',
             'KAIMANA',
             'MANOKWARI SELATAN',
             'PEGUNUNGAN ARFAK',
             'TELUK BINTUNI',
             'TELUK WONDAMA'
         ) THEN 'MANOKWARI OUTER'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'KOTA SORONG',
             'MAYBRAT',
             'RAJA AMPAT',
             'SORONG',
             'SORONG SELATAN',
             'TAMBRAUW'
         ) THEN 'NEW SORONG RAJA AMPAT'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN (
             'INTAN JAYA',
             'MIMIKA',
             'PUNCAK',
             'PUNCAK JAYA',
             'TIMIKA'
         ) THEN 'MIMIKA PUNCAK'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
         WHEN ${prevYearCurrMonthRevSA.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
         ELSE NULL
     END
            `.as('clusterName'),
                kabupaten: prevYearCurrMonthRevSA.kabupaten,
                price: prevYearCurrMonthRevSA.price,
                trx: sql<number>`COUNT(${prevYearCurrMonthRevSA.msisdn})`.as('trx')
            })
            .from(prevYearCurrMonthRevSA)
            .where(and(
                not(eq(prevYearCurrMonthRevSA.kabupaten, 'TMP')),
                and(
                    inArray(prevYearCurrMonthRevSA.regional, ['MALUKU DAN PAPUA', 'PUMA']),
                    between(prevYearCurrMonthRevSA.trxDate, firstDayOfPrevYearCurrMonth, prevYearCurrDate)
                )
            ))
            .groupBy(sql`1,2,3,4,5`)
            .as('sq2')

        const regClassP4 = db5.select({
            regionName: sq4.regionName,
            branchName: sq4.branchName,
            subbranchName: sq4.subbranchName,
            clusterName: sq4.clusterName,
            cityName: sq4.kabupaten,
            revenue: sql<number>`SUM(${sq4.price} * ${sq4.trx})`.as('revenue')
        })
            .from(sq4)
            .groupBy(sql`1,2,3,4,5`)
            .as('regionClassififcation')

        // QUERY UNTUK TARGET BULAN INI
        const p1 = db
            .select({
                id: regionals.id,
                region: regionals.regional,
                branch: branches.branchNew,
                subbranch: subbranches.subbranchNew,
                cluster: clusters.cluster,
                kabupaten: kabupatens.kabupaten,
                currMonthTargetRev: sql<number>`SUM(${revenueSA[monthColumn]})`.as('currMonthTargetRev')
            })
            .from(regionals)
            .leftJoin(branches, eq(regionals.id, branches.regionalId))
            .leftJoin(subbranches, eq(branches.id, subbranches.branchId))
            .leftJoin(clusters, eq(subbranches.id, clusters.subbranchId))
            .leftJoin(kabupatens, eq(clusters.id, kabupatens.clusterId))
            .leftJoin(revenueSA, eq(kabupatens.id, revenueSA.kabupatenId))
            .groupBy(
                regionals.regional,
                branches.branchNew,
                subbranches.subbranchNew,
                clusters.cluster,
                kabupatens.kabupaten
            )
            .orderBy(asc(regionals.regional))
            .prepare()

        //  QUERY UNTUK MENDAPAT CURRENT MONTH REVENUE (Mtd)
        const p2 = db5
            .select({
                region: sql<string>`${regClassP2.regionName}`.as('region'),
                branch: sql<string>`${regClassP2.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${regClassP2.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${regClassP2.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${regClassP2.cityName}`.as('kabupaten'),
                currMonthKabupatenRev: sql<number>`SUM(${regClassP2.revenue})`.as('currMonthKabupatenRev'),
                currMonthClusterRev: sql<number>`SUM(SUM(${regClassP2.revenue})) OVER (PARTITION BY ${regClassP2.regionName}, ${regClassP2.branchName}, ${regClassP2.subbranchName}, ${regClassP2.clusterName})`.as('currMonthClusterRev'),
                currMonthSubbranchRev: sql<number>`SUM(SUM(${regClassP2.revenue})) OVER (PARTITION BY ${regClassP2.regionName}, ${regClassP2.branchName}, ${regClassP2.subbranchName})`.as('currMonthSubbranchRev'),
                currMonthBranchRev: sql<number>`SUM(SUM(${regClassP2.revenue})) OVER (PARTITION BY ${regClassP2.regionName}, ${regClassP2.branchName})`.as('currMonthBranchRev'),
                currMonthRegionalRev: sql<number>`SUM(SUM(${regClassP2.revenue})) OVER (PARTITION BY ${regClassP2.regionName})`.as('currMonthRegionalRev')
            })
            .from(regClassP2)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        // QUERY UNTUK MENDAPAT PREV MONTH REVENUE
        const p3 = db5
            .select({
                region: sql<string>`${regClassP3.regionName}`.as('region'),
                branch: sql<string>`${regClassP3.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${regClassP3.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${regClassP3.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${regClassP3.cityName}`.as('kabupaten'),
                prevMonthKabupatenRev: sql<number>`SUM(${regClassP3.revenue})`.as('currMonthKabupatenRev'),
                prevMonthClusterRev: sql<number>`SUM(SUM(${regClassP3.revenue})) OVER (PARTITION BY ${regClassP3.regionName}, ${regClassP3.branchName}, ${regClassP3.subbranchName}, ${regClassP3.clusterName})`.as('currMonthClusterRev'),
                prevMonthSubbranchRev: sql<number>`SUM(SUM(${regClassP3.revenue})) OVER (PARTITION BY ${regClassP3.regionName}, ${regClassP3.branchName}, ${regClassP3.subbranchName})`.as('currMonthSubbranchRev'),
                prevMonthBranchRev: sql<number>`SUM(SUM(${regClassP3.revenue})) OVER (PARTITION BY ${regClassP3.regionName}, ${regClassP3.branchName})`.as('currMonthBranchRev'),
                prevMonthRegionalRev: sql<number>`SUM(SUM(${regClassP3.revenue})) OVER (PARTITION BY ${regClassP3.regionName})`.as('currMonthRegionalRev')
            })
            .from(regClassP3)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        // QUERY UNTUK MENDAPAT PREV YEAR CURR MONTH REVENUE
        const p4 = db5
            .select({
                region: sql<string>`${regClassP4.regionName}`.as('region'),
                branch: sql<string>`${regClassP4.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${regClassP4.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${regClassP4.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${regClassP4.cityName}`.as('kabupaten'),
                prevYearCurrMonthKabupatenRev: sql<number>`SUM(${regClassP4.revenue})`.as('currMonthKabupatenRev'),
                prevYearCurrMonthClusterRev: sql<number>`SUM(SUM(${regClassP4.revenue})) OVER (PARTITION BY ${regClassP4.regionName}, ${regClassP4.branchName}, ${regClassP4.subbranchName}, ${regClassP4.clusterName})`.as('currMonthClusterRev'),
                prevYearCurrMonthSubbranchRev: sql<number>`SUM(SUM(${regClassP4.revenue})) OVER (PARTITION BY ${regClassP4.regionName}, ${regClassP4.branchName}, ${regClassP4.subbranchName})`.as('currMonthSubbranchRev'),
                prevYearCurrMonthBranchRev: sql<number>`SUM(SUM(${regClassP4.revenue})) OVER (PARTITION BY ${regClassP4.regionName}, ${regClassP4.branchName})`.as('currMonthBranchRev'),
                prevYearCurrMonthRegionalRev: sql<number>`SUM(SUM(${regClassP4.revenue})) OVER (PARTITION BY ${regClassP4.regionName})`.as('currMonthRegionalRev')
            })
            .from(regClassP4)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        // QUERY UNTUK YtD 2025

        const [targetRevenue, currMonthRevenue, prevMonthRevenue, prevYearCurrMonthRevenue] = await Promise.all([
            p1.execute(),
            p2.execute(),
            p3.execute(),
            p4.execute()
        ])
        // /var/lib/backup_mysql_2025/
        const regionalsMap = new Map();

        targetRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthTarget += Number(row.currMonthTargetRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize kabupaten if it doesn't exist
            cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: Number(row.currMonthTargetRev),
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
        })

        currMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthRevenue = Number(row.currMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.currMonthRevenue = Number(row.currMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.currMonthRevenue = Number(row.currMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.currMonthRevenue = Number(row.currMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));

            kabupaten.currMonthRevenue = Number(row.currMonthKabupatenRev)
        })

        prevMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevMonthRevenue = Number(row.prevMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.prevMonthRevenue = Number(row.prevMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.prevMonthRevenue = Number(row.prevMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevMonthRevenue = Number(row.prevMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
            kabupaten.prevMonthRevenue = Number(row.prevMonthKabupatenRev)
        })

        prevYearCurrMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
            kabupaten.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthKabupatenRev)
        })

        const finalDataRevenue = Array.from(regionalsMap.values()).map((regional: any) => ({
            ...regional,
            branches: Array.from(regional.branches.values()).map((branch: any) => ({
                ...branch,
                subbranches: Array.from(branch.subbranches.values()).map((subbranch: any) => ({
                    ...subbranch,
                    clusters: Array.from(subbranch.clusters.values().map((cluster: any) => ({
                        ...cluster,
                        kabupatens: Array.from(cluster.kabupatens.values())
                    }))),
                })),
            })),
        }));

        return Response.json({ data: finalDataRevenue }, { status: 200 })
    }, {
        query: t.Object({
            date: t.Optional(t.Date())
        })
    })
    .get('/paying-los', async ({ db, db2, query }) => {
        const { date } = query

        const selectedDate = date ? new Date(date) : new Date()
        const month = (subDays(selectedDate, 2).getMonth() + 1).toString()

        // KOLOM DINAMIS UNTUK MEMILIH ANTARA KOLOM `m1-m12`
        const monthColumn = `m${month}` as keyof typeof payingLOS_01.$inferSelect

        // VARIABLE TANGGAL UNTUK IMPORT TABEL SECARA DINAMIS
        const latestDataDate = subDays(selectedDate, 2);

        const currMonth = format(latestDataDate, 'MM')
        const currYear = format(latestDataDate, 'yyyy')
        const isPrevMonthLastYear = currMonth === '01'
        const prevMonth = isPrevMonthLastYear ? '12' : format(subMonths(latestDataDate, 1), 'MM')
        const prevMonthYear = isPrevMonthLastYear ? format(subYears(latestDataDate, 1), 'yyyy') : format(latestDataDate, 'yyyy')
        const prevYear = format(subYears(latestDataDate, 1), 'yyyy')

        // TABEL DINAMIS
        const currRevSubs = dynamicCbProfileTable(currYear, currMonth)
        const prevMonthRevSubs = dynamicCbProfileTable(prevMonthYear, prevMonth)
        const prevYearCurrMonthRevSubs = dynamicCbProfileTable(prevYear, currMonth)

        // VARIABLE TANGGAL
        const firstDayOfCurrMonth = format(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 'yyyy-MM-dd')
        const firstDayOfPrevMonth = format(subMonths(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const firstDayOfPrevYearCurrMonth = format(subYears(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const currDate = format(latestDataDate, 'yyyy-MM-dd')
        const prevDate = format(subMonths(latestDataDate, 1), 'yyyy-MM-dd')
        const prevYearCurrDate = format(subYears(latestDataDate, 1), 'yyyy-MM-dd')

        const sq2 = db2
            .select({
                regionName: sql<string>`'PUMA'`.as('regionName'),
                branchName: sql<string>`
CASE
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR',
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'BURU',
     'BURU SELATAN',
     'SERAM BAGIAN BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'AMBON'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'KOTA JAYAPURA',
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'JAYAPURA'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'MANOKWARI',
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA',
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'ASMAT',
     'BOVEN DIGOEL',
     'MAPPI',
     'MERAUKE',
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA',
     'DEIYAI',
     'DOGIYAI',
     'NABIRE',
     'PANIAI'
 ) THEN 'TIMIKA'
 ELSE NULL
END
                    `.as('branchName'),
                subbranchName: sql<string>`
CASE
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN AMBON'
 WHEN upper(${currRevSubs.kabupaten}) IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
 WHEN upper(${currRevSubs.kabupaten}) IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'SENTANI'
 WHEN upper(${currRevSubs.kabupaten}) IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG RAJA AMPAT'
 WHEN upper(${currRevSubs.kabupaten}) IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA'
 WHEN upper(${currRevSubs.kabupaten}) IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 ELSE NULL
END
                    `.as('subbranchName'),
                clusterName: sql<string>`
CASE
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN TUAL'
 WHEN upper(${currRevSubs.kabupaten}) IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
 WHEN upper(${currRevSubs.kabupaten}) IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
 WHEN upper(${currRevSubs.kabupaten}) IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN'
 ) THEN 'NEW BIAK NUMFOR'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'PAPUA PEGUNUNGAN'
 WHEN upper(${currRevSubs.kabupaten}) IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'NEW SORONG RAJA AMPAT'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA PUNCAK'
 WHEN upper(${currRevSubs.kabupaten}) IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 WHEN upper(${currRevSubs.kabupaten}) IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
 ELSE NULL
END
                    `.as('clusterName'),
                kabupaten: currRevSubs.kabupaten,
                revenue: currRevSubs.REVMTD
            })
            .from(currRevSubs)
            .where(and(
                eq(currRevSubs.flagLoS, 'SALES N'),
                and(
                    eq(currRevSubs.flagRGB, 'RGB'),
                    inArray(currRevSubs.branch, ['AMBON', 'SORONG', 'JAYAPURA', 'TIMIKA'])
                )
            ))
            .as('sq2')

        const sq3 = db2
            .select({
                regionName: sql<string>`'PUMA'`.as('regionName'),
                branchName: sql<string>`
CASE
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR',
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'BURU',
     'BURU SELATAN',
     'SERAM BAGIAN BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'AMBON'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'KOTA JAYAPURA',
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'JAYAPURA'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'MANOKWARI',
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA',
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'ASMAT',
     'BOVEN DIGOEL',
     'MAPPI',
     'MERAUKE',
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA',
     'DEIYAI',
     'DOGIYAI',
     'NABIRE',
     'PANIAI'
 ) THEN 'TIMIKA'
 ELSE NULL
END
                    `.as('branchName'),
                subbranchName: sql<string>`
CASE
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN AMBON'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'SENTANI'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG RAJA AMPAT'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 ELSE NULL
END
                    `.as('subbranchName'),
                clusterName: sql<string>`
CASE
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN TUAL'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN'
 ) THEN 'NEW BIAK NUMFOR'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'PAPUA PEGUNUNGAN'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'NEW SORONG RAJA AMPAT'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA PUNCAK'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
 ELSE NULL
END
                    `.as('clusterName'),
                kabupaten: prevMonthRevSubs.kabupaten,
                revenue: prevMonthRevSubs.REVMTD
            })
            .from(prevMonthRevSubs)
            .where(and(
                eq(prevMonthRevSubs.flagLoS, 'SALES N'),
                and(
                    eq(prevMonthRevSubs.flagRGB, 'RGB'),
                    inArray(prevMonthRevSubs.branch, ['AMBON', 'SORONG', 'JAYAPURA', 'TIMIKA'])
                )
            ))
            .as('sq3')

        const sq4 = db2
            .select({
                regionName: sql<string>`'PUMA'`.as('regionName'),
                branchName: sql<string>`
CASE
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR',
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'BURU',
     'BURU SELATAN',
     'SERAM BAGIAN BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'AMBON'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'KOTA JAYAPURA',
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'JAYAPURA'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'MANOKWARI',
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA',
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'ASMAT',
     'BOVEN DIGOEL',
     'MAPPI',
     'MERAUKE',
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA',
     'DEIYAI',
     'DOGIYAI',
     'NABIRE',
     'PANIAI'
 ) THEN 'TIMIKA'
 ELSE NULL
END
                    `.as('branchName'),
                subbranchName: sql<string>`
CASE
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN AMBON'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'SENTANI'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG RAJA AMPAT'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 ELSE NULL
END
                    `.as('subbranchName'),
                clusterName: sql<string>`
CASE
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN TUAL'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN'
 ) THEN 'NEW BIAK NUMFOR'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'PAPUA PEGUNUNGAN'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'NEW SORONG RAJA AMPAT'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA PUNCAK'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
 ELSE NULL
END
                    `.as('clusterName'),
                kabupaten: prevYearCurrMonthRevSubs.kabupaten,
                revenue: prevYearCurrMonthRevSubs.REVMTD
            })
            .from(prevYearCurrMonthRevSubs)
            .where(and(
                eq(prevYearCurrMonthRevSubs.flagRGB, 'SALES N'),
                and(
                    eq(prevYearCurrMonthRevSubs.flagRGB, 'RGB'),
                    inArray(prevYearCurrMonthRevSubs.branch, ['AMBON', 'SORONG', 'JAYAPURA', 'TIMIKA'])
                )
            ))
            .as('sq4')

        // QUERY UNTUK TARGET BULAN INI
        const p1 = db
            .select({
                id: regionals.id,
                region: regionals.regional,
                branch: branches.branchNew,
                subbranch: subbranches.subbranchNew,
                cluster: clusters.cluster,
                kabupaten: kabupatens.kabupaten,
                currMonthTargetRev: sql<number>`CAST(SUM(${payingLOS_01[monthColumn]}) AS DOUBLE PRECISION)`.as('currMonthTargetRev')
            })
            .from(regionals)
            .leftJoin(branches, eq(regionals.id, branches.regionalId))
            .leftJoin(subbranches, eq(branches.id, subbranches.branchId))
            .leftJoin(clusters, eq(subbranches.id, clusters.subbranchId))
            .leftJoin(kabupatens, eq(clusters.id, kabupatens.clusterId))
            .leftJoin(payingLOS_01, eq(kabupatens.id, payingLOS_01.kabupatenId))
            .groupBy(
                regionals.regional,
                branches.branchNew,
                subbranches.subbranchNew,
                clusters.cluster,
                kabupatens.kabupaten
            )
            .orderBy(asc(regionals.regional))
            .prepare()

        //  QUERY UNTUK MENDAPAT CURRENT MONTH REVENUE (Mtd)
        const p2 = db2
            .select({
                region: sql<string>`${sq2.regionName}`.as('region'),
                branch: sql<string>`${sq2.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq2.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq2.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq2.kabupaten}`.as('kabupaten'),
                currMonthKabupatenRev: sql<number>`SUM(${sq2.revenue})`.as('currMonthKabupatenRev'),
                currMonthClusterRev: sql<number>`SUM(SUM(${sq2.revenue})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName}, ${sq2.subbranchName}, ${sq2.clusterName})`.as('currMonthClusterRev'),
                currMonthSubbranchRev: sql<number>`SUM(SUM(${sq2.revenue})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName}, ${sq2.subbranchName})`.as('currMonthSubbranchRev'),
                currMonthBranchRev: sql<number>`SUM(SUM(${sq2.revenue})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName})`.as('currMonthBranchRev'),
                currMonthRegionalRev: sql<number>`SUM(SUM(${sq2.revenue})) OVER (PARTITION BY ${sq2.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq2)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()


        // QUERY UNTUK MENDAPAT PREV MONTH REVENUE
        const p3 = db2
            .select({
                region: sql<string>`${sq3.regionName}`.as('region'),
                branch: sql<string>`${sq3.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq3.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq3.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq3.kabupaten}`.as('kabupaten'),
                prevMonthKabupatenRev: sql<number>`SUM(${sq3.revenue})`.as('currMonthKabupatenRev'),
                prevMonthClusterRev: sql<number>`SUM(SUM(${sq3.revenue})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName}, ${sq3.subbranchName}, ${sq3.clusterName})`.as('currMonthClusterRev'),
                prevMonthSubbranchRev: sql<number>`SUM(SUM(${sq3.revenue})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName}, ${sq3.subbranchName})`.as('currMonthSubbranchRev'),
                prevMonthBranchRev: sql<number>`SUM(SUM(${sq3.revenue})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName})`.as('currMonthBranchRev'),
                prevMonthRegionalRev: sql<number>`SUM(SUM(${sq3.revenue})) OVER (PARTITION BY ${sq3.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq3)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        // QUERY UNTUK MENDAPAT PREV YEAR CURR MONTH REVENUE
        const p4 = db2
            .select({
                region: sql<string>`${sq4.regionName}`.as('region'),
                branch: sql<string>`${sq4.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq4.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq4.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq4.kabupaten}`.as('kabupaten'),
                prevYearCurrMonthKabupatenRev: sql<number>`SUM(${sq4.revenue})`.as('currMonthKabupatenRev'),
                prevYearCurrMonthClusterRev: sql<number>`SUM(SUM(${sq4.revenue})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName}, ${sq4.subbranchName}, ${sq4.clusterName})`.as('currMonthClusterRev'),
                prevYearCurrMonthSubbranchRev: sql<number>`SUM(SUM(${sq4.revenue})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName}, ${sq4.subbranchName})`.as('currMonthSubbranchRev'),
                prevYearCurrMonthBranchRev: sql<number>`SUM(SUM(${sq4.revenue})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName})`.as('currMonthBranchRev'),
                prevYearCurrMonthRegionalRev: sql<number>`SUM(SUM(${sq4.revenue})) OVER (PARTITION BY ${sq4.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq4)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        const [targetRevenue, currMonthRevenue, prevMonthRevenue, prevYearCurrMonthRevenue] = await Promise.all([
            p1.execute(),
            p2.execute(),
            p3.execute(),
            p4.execute()
        ])

        const regionalsMap = new Map();

        targetRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthTarget += Number(row.currMonthTargetRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize kabupaten if it doesn't exist
            cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: Number(row.currMonthTargetRev),
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
        })

        currMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthRevenue = Number(row.currMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.currMonthRevenue = Number(row.currMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.currMonthRevenue = Number(row.currMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.currMonthRevenue = Number(row.currMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));

            kabupaten.currMonthRevenue = Number(row.currMonthKabupatenRev)
        })

        prevMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevMonthRevenue = Number(row.prevMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.prevMonthRevenue = Number(row.prevMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.prevMonthRevenue = Number(row.prevMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevMonthRevenue = Number(row.prevMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
            kabupaten.prevMonthRevenue = Number(row.prevMonthKabupatenRev)
        })

        prevYearCurrMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
            kabupaten.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthKabupatenRev)
        })

        const finalDataRevenue = Array.from(regionalsMap.values()).map((regional: any) => ({
            ...regional,
            branches: Array.from(regional.branches.values()).map((branch: any) => ({
                ...branch,
                subbranches: Array.from(branch.subbranches.values()).map((subbranch: any) => ({
                    ...subbranch,
                    clusters: Array.from(subbranch.clusters.values().map((cluster: any) => ({
                        ...cluster,
                        kabupatens: Array.from(cluster.kabupatens.values())
                    }))),
                })),
            })),
        }));

        return Response.json({ data: finalDataRevenue }, { status: 200 })
    }, {
        query: t.Object({
            date: t.Optional(t.Date())
        })
    })
    .get('/paying-subs', async ({ query, db, db2 }) => {
        const { date } = query
        const selectedDate = date ? new Date(date) : new Date()
        const month = (subDays(selectedDate, 2).getMonth() + 1).toString()

        // KOLOM DINAMIS UNTUK MEMILIH ANTARA KOLOM `m1-m12`
        const monthColumn = `m${month}` as keyof typeof payingSubs.$inferSelect

        // VARIABLE TANGGAL UNTUK IMPORT TABEL SECARA DINAMIS
        const latestDataDate = subDays(selectedDate, 2);

        const currMonth = format(latestDataDate, 'MM')
        const currYear = format(latestDataDate, 'yyyy')
        const isPrevMonthLastYear = currMonth === '01'
        const prevMonth = isPrevMonthLastYear ? '12' : format(subMonths(latestDataDate, 1), 'MM')
        const prevMonthYear = isPrevMonthLastYear ? format(subYears(latestDataDate, 1), 'yyyy') : format(latestDataDate, 'yyyy')
        const prevYear = format(subYears(latestDataDate, 1), 'yyyy')

        // TABEL DINAMIS
        const currRevSubs = dynamicCbProfileTable(currYear, currMonth)
        const prevMonthRevSubs = dynamicCbProfileTable(prevMonthYear, prevMonth)
        const prevYearCurrMonthRevSubs = dynamicCbProfileTable(prevYear, currMonth)

        // VARIABLE TANGGAL
        const firstDayOfCurrMonth = format(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 'yyyy-MM-dd')
        const firstDayOfPrevMonth = format(subMonths(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const firstDayOfPrevYearCurrMonth = format(subYears(new Date(latestDataDate.getFullYear(), latestDataDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const currDate = format(latestDataDate, 'yyyy-MM-dd')
        const prevDate = format(subMonths(latestDataDate, 1), 'yyyy-MM-dd')
        const prevYearCurrDate = format(subYears(latestDataDate, 1), 'yyyy-MM-dd')

        const sq2 = db2
            .select({
                regionName: sql<string>`'PUMA'`.as('regionName'),
                branchName: sql<string>`
CASE
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR',
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'BURU',
     'BURU SELATAN',
     'SERAM BAGIAN BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'AMBON'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'KOTA JAYAPURA',
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'JAYAPURA'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'MANOKWARI',
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA',
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'ASMAT',
     'BOVEN DIGOEL',
     'MAPPI',
     'MERAUKE',
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA',
     'DEIYAI',
     'DOGIYAI',
     'NABIRE',
     'PANIAI'
 ) THEN 'TIMIKA'
 ELSE NULL
END
                    `.as('branchName'),
                subbranchName: sql<string>`
CASE
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN AMBON'
 WHEN upper(${currRevSubs.kabupaten}) IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
 WHEN upper(${currRevSubs.kabupaten}) IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'SENTANI'
 WHEN upper(${currRevSubs.kabupaten}) IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG RAJA AMPAT'
 WHEN upper(${currRevSubs.kabupaten}) IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA'
 WHEN upper(${currRevSubs.kabupaten}) IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 ELSE NULL
END
                    `.as('subbranchName'),
                clusterName: sql<string>`
CASE
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN TUAL'
 WHEN upper(${currRevSubs.kabupaten}) IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
 WHEN upper(${currRevSubs.kabupaten}) IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
 WHEN upper(${currRevSubs.kabupaten}) IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN'
 ) THEN 'NEW BIAK NUMFOR'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'PAPUA PEGUNUNGAN'
 WHEN upper(${currRevSubs.kabupaten}) IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'NEW SORONG RAJA AMPAT'
 WHEN upper(${currRevSubs.kabupaten}) IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA PUNCAK'
 WHEN upper(${currRevSubs.kabupaten}) IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 WHEN upper(${currRevSubs.kabupaten}) IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
 ELSE NULL
END
                    `.as('clusterName'),
                kabupaten: currRevSubs.kabupaten,
                revenue: currRevSubs.REVMTD
            })
            .from(currRevSubs)
            .where(and(
                eq(currRevSubs.flagRGB, 'RGB'),
                inArray(currRevSubs.branch, ['AMBON', 'SORONG', 'JAYAPURA', 'TIMIKA'])
            ))
            .as('sq2')

        const sq3 = db2
            .select({
                regionName: sql<string>`'PUMA'`.as('regionName'),
                branchName: sql<string>`
CASE
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR',
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'BURU',
     'BURU SELATAN',
     'SERAM BAGIAN BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'AMBON'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'KOTA JAYAPURA',
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'JAYAPURA'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'MANOKWARI',
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA',
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'ASMAT',
     'BOVEN DIGOEL',
     'MAPPI',
     'MERAUKE',
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA',
     'DEIYAI',
     'DOGIYAI',
     'NABIRE',
     'PANIAI'
 ) THEN 'TIMIKA'
 ELSE NULL
END
                    `.as('branchName'),
                subbranchName: sql<string>`
CASE
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN AMBON'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'SENTANI'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG RAJA AMPAT'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 ELSE NULL
END
                    `.as('subbranchName'),
                clusterName: sql<string>`
CASE
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN TUAL'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN'
 ) THEN 'NEW BIAK NUMFOR'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'PAPUA PEGUNUNGAN'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'NEW SORONG RAJA AMPAT'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA PUNCAK'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 WHEN upper(${prevMonthRevSubs.kabupaten}) IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
 ELSE NULL
END
                    `.as('clusterName'),
                kabupaten: prevMonthRevSubs.kabupaten,
                revenue: prevMonthRevSubs.REVMTD
            })
            .from(prevMonthRevSubs)
            .where(and(
                eq(prevMonthRevSubs.flagRGB, 'RGB'),
                inArray(prevMonthRevSubs.branch, ['AMBON', 'SORONG', 'JAYAPURA', 'TIMIKA'])
            ))
            .as('sq3')

        const sq4 = db2
            .select({
                regionName: sql<string>`'PUMA'`.as('regionName'),
                branchName: sql<string>`
CASE
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR',
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'BURU',
     'BURU SELATAN',
     'SERAM BAGIAN BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'AMBON'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'KOTA JAYAPURA',
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'JAYAPURA'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'MANOKWARI',
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA',
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'ASMAT',
     'BOVEN DIGOEL',
     'MAPPI',
     'MERAUKE',
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA',
     'DEIYAI',
     'DOGIYAI',
     'NABIRE',
     'PANIAI'
 ) THEN 'TIMIKA'
 ELSE NULL
END
                    `.as('branchName'),
                subbranchName: sql<string>`
CASE
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'AMBON',
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN AMBON'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'JAYAPURA',
     'KEEROM',
     'MAMBERAMO RAYA',
     'SARMI',
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN',
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'SENTANI'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'SORONG RAJA AMPAT'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 ELSE NULL
END
                    `.as('subbranchName'),
                clusterName: sql<string>`
CASE
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'KOTA AMBON',
     'MALUKU TENGAH',
     'SERAM BAGIAN TIMUR'
 ) THEN 'AMBON'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'KEPULAUAN ARU',
     'KOTA TUAL',
     'MALUKU BARAT DAYA',
     'MALUKU TENGGARA',
     'MALUKU TENGGARA BARAT',
     'KEPULAUAN TANIMBAR'
 ) THEN 'KEPULAUAN TUAL'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'BIAK',
     'BIAK NUMFOR',
     'KEPULAUAN YAPEN',
     'SUPIORI',
     'WAROPEN'
 ) THEN 'NEW BIAK NUMFOR'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'JAYAWIJAYA',
     'LANNY JAYA',
     'MAMBERAMO TENGAH',
     'NDUGA',
     'PEGUNUNGAN BINTANG',
     'TOLIKARA',
     'YAHUKIMO',
     'YALIMO'
 ) THEN 'PAPUA PEGUNUNGAN'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('MANOKWARI') THEN 'MANOKWARI'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'FAKFAK',
     'FAK FAK',
     'KAIMANA',
     'MANOKWARI SELATAN',
     'PEGUNUNGAN ARFAK',
     'TELUK BINTUNI',
     'TELUK WONDAMA'
 ) THEN 'MANOKWARI OUTER'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'KOTA SORONG',
     'MAYBRAT',
     'RAJA AMPAT',
     'SORONG',
     'SORONG SELATAN',
     'TAMBRAUW'
 ) THEN 'NEW SORONG RAJA AMPAT'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN (
     'INTAN JAYA',
     'MIMIKA',
     'PUNCAK',
     'PUNCAK JAYA',
     'TIMIKA'
 ) THEN 'MIMIKA PUNCAK'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
 WHEN upper(${prevYearCurrMonthRevSubs.kabupaten}) IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
 ELSE NULL
END
                    `.as('clusterName'),
                kabupaten: prevYearCurrMonthRevSubs.kabupaten,
                revenue: prevYearCurrMonthRevSubs.REVMTD
            })
            .from(prevYearCurrMonthRevSubs)
            .where(and(
                eq(prevYearCurrMonthRevSubs.flagRGB, 'RGB'),
                inArray(prevYearCurrMonthRevSubs.branch, ['AMBON', 'SORONG', 'JAYAPURA', 'TIMIKA'])
            ))
            .as('sq4')

        // QUERY UNTUK TARGET BULAN INI
        const p1 = db
            .select({
                id: regionals.id,
                region: regionals.regional,
                branch: branches.branchNew,
                subbranch: subbranches.subbranchNew,
                cluster: clusters.cluster,
                kabupaten: kabupatens.kabupaten,
                currMonthTargetRev: sql<number>`CAST(SUM(${payingSubs[monthColumn]}) AS DOUBLE PRECISION)`.as('currMonthTargetRev')
            })
            .from(regionals)
            .leftJoin(branches, eq(regionals.id, branches.regionalId))
            .leftJoin(subbranches, eq(branches.id, subbranches.branchId))
            .leftJoin(clusters, eq(subbranches.id, clusters.subbranchId))
            .leftJoin(kabupatens, eq(clusters.id, kabupatens.clusterId))
            .leftJoin(payingSubs, eq(kabupatens.id, payingSubs.kabupatenId))
            .groupBy(
                regionals.regional,
                branches.branchNew,
                subbranches.subbranchNew,
                clusters.cluster,
                kabupatens.kabupaten
            )
            .orderBy(asc(regionals.regional))
            .prepare()

        //  QUERY UNTUK MENDAPAT CURRENT MONTH REVENUE (Mtd)
        const p2 = db2
            .select({
                region: sql<string>`${sq2.regionName}`.as('region'),
                branch: sql<string>`${sq2.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq2.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq2.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq2.kabupaten}`.as('kabupaten'),
                currMonthKabupatenRev: sql<number>`SUM(${sq2.revenue})`.as('currMonthKabupatenRev'),
                currMonthClusterRev: sql<number>`SUM(SUM(${sq2.revenue})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName}, ${sq2.subbranchName}, ${sq2.clusterName})`.as('currMonthClusterRev'),
                currMonthSubbranchRev: sql<number>`SUM(SUM(${sq2.revenue})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName}, ${sq2.subbranchName})`.as('currMonthSubbranchRev'),
                currMonthBranchRev: sql<number>`SUM(SUM(${sq2.revenue})) OVER (PARTITION BY ${sq2.regionName}, ${sq2.branchName})`.as('currMonthBranchRev'),
                currMonthRegionalRev: sql<number>`SUM(SUM(${sq2.revenue})) OVER (PARTITION BY ${sq2.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq2)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()


        // QUERY UNTUK MENDAPAT PREV MONTH REVENUE
        const p3 = db2
            .select({
                region: sql<string>`${sq3.regionName}`.as('region'),
                branch: sql<string>`${sq3.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq3.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq3.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq3.kabupaten}`.as('kabupaten'),
                prevMonthKabupatenRev: sql<number>`SUM(${sq3.revenue})`.as('currMonthKabupatenRev'),
                prevMonthClusterRev: sql<number>`SUM(SUM(${sq3.revenue})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName}, ${sq3.subbranchName}, ${sq3.clusterName})`.as('currMonthClusterRev'),
                prevMonthSubbranchRev: sql<number>`SUM(SUM(${sq3.revenue})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName}, ${sq3.subbranchName})`.as('currMonthSubbranchRev'),
                prevMonthBranchRev: sql<number>`SUM(SUM(${sq3.revenue})) OVER (PARTITION BY ${sq3.regionName}, ${sq3.branchName})`.as('currMonthBranchRev'),
                prevMonthRegionalRev: sql<number>`SUM(SUM(${sq3.revenue})) OVER (PARTITION BY ${sq3.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq3)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        // QUERY UNTUK MENDAPAT PREV YEAR CURR MONTH REVENUE
        const p4 = db2
            .select({
                region: sql<string>`${sq4.regionName}`.as('region'),
                branch: sql<string>`${sq4.branchName}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${sq4.subbranchName}`.as('subbranch'),
                cluster: sql<string>`${sq4.clusterName}`.as('cluster'),
                kabupaten: sql<string>`${sq4.kabupaten}`.as('kabupaten'),
                prevYearCurrMonthKabupatenRev: sql<number>`SUM(${sq4.revenue})`.as('currMonthKabupatenRev'),
                prevYearCurrMonthClusterRev: sql<number>`SUM(SUM(${sq4.revenue})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName}, ${sq4.subbranchName}, ${sq4.clusterName})`.as('currMonthClusterRev'),
                prevYearCurrMonthSubbranchRev: sql<number>`SUM(SUM(${sq4.revenue})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName}, ${sq4.subbranchName})`.as('currMonthSubbranchRev'),
                prevYearCurrMonthBranchRev: sql<number>`SUM(SUM(${sq4.revenue})) OVER (PARTITION BY ${sq4.regionName}, ${sq4.branchName})`.as('currMonthBranchRev'),
                prevYearCurrMonthRegionalRev: sql<number>`SUM(SUM(${sq4.revenue})) OVER (PARTITION BY ${sq4.regionName})`.as('currMonthRegionalRev')
            })
            .from(sq4)
            .groupBy(sql`1,2,3,4,5`)
            .prepare()

        const [targetRevenue, currMonthRevenue, prevMonthRevenue, prevYearCurrMonthRevenue] = await Promise.all([
            p1.execute(),
            p2.execute(),
            p3.execute(),
            p4.execute()
        ])

        const regionalsMap = new Map();

        targetRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthTarget += Number(row.currMonthTargetRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.currMonthTarget += Number(row.currMonthTargetRev)

            // Initialize kabupaten if it doesn't exist
            cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: Number(row.currMonthTargetRev),
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
        })

        currMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthRevenue = Number(row.currMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.currMonthRevenue = Number(row.currMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.currMonthRevenue = Number(row.currMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.currMonthRevenue = Number(row.currMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));

            kabupaten.currMonthRevenue = Number(row.currMonthKabupatenRev)
        })

        prevMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevMonthRevenue = Number(row.prevMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.prevMonthRevenue = Number(row.prevMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.prevMonthRevenue = Number(row.prevMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevMonthRevenue = Number(row.prevMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
            kabupaten.prevMonthRevenue = Number(row.prevMonthKabupatenRev)
        })

        prevYearCurrMonthRevenue.forEach((row) => {
            const regionalName = row.region;
            const branchName = row.branch;
            const subbranchName = row.subbranch;
            const clusterName = row.cluster;
            const kabupatenName = row.kabupaten;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthRevenue: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            const kabupaten = cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthRevenue: 0
                }), cluster.kabupatens.get(kabupatenName));
            kabupaten.prevYearCurrMonthRevenue = Number(row.prevYearCurrMonthKabupatenRev)
        })

        const finalDataRevenue = Array.from(regionalsMap.values()).map((regional: any) => ({
            ...regional,
            branches: Array.from(regional.branches.values()).map((branch: any) => ({
                ...branch,
                subbranches: Array.from(branch.subbranches.values()).map((subbranch: any) => ({
                    ...subbranch,
                    clusters: Array.from(subbranch.clusters.values().map((cluster: any) => ({
                        ...cluster,
                        kabupatens: Array.from(cluster.kabupatens.values())
                    }))),
                })),
            })),
        }));

        return Response.json({ data: finalDataRevenue }, { status: 200 })
    }, {
        query: t.Object({
            date: t.Optional(t.Date())
        })
    })
    .onError(({ code }) => {
        if (code === 'NOT_FOUND') {
            return Response.json({ message: 'Route not found :(' }, { status: 404 })
        }
    })
    .listen(8000)

console.log(`Running on http://localhost:8000`);
