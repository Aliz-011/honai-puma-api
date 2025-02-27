import { Elysia, t } from "elysia";
import { swagger } from '@elysiajs/swagger'
import { and, asc, between, eq, isNotNull, sql } from "drizzle-orm";
import { subMonths, subDays, format, subYears } from 'date-fns'
import { index, type AnyMySqlTable } from "drizzle-orm/mysql-core";

import { db as conn, db2 as conn2, db3 as conn3 } from '../database'
import { dynamicResumeRevenuePumaTable } from "../database/schema2";
import { dynamicByuTable } from "../database/schema3";
import { table } from "../database/schema";
import { model } from '../database/model'

const { regionals, branches, clusters, kabupatens, subbranches, revenueGrosses, revenueByu } = table

new Elysia({ prefix: '/api', serve: { idleTimeout: 255 } })
    .decorate('db', conn)
    .decorate('db2', conn2)
    .decorate('db3', conn3)
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
    .get('/revenue-gross-prabayar', async ({ db, db2, query }) => {
        const { date, branch, cluster, kabupaten, subbranch } = query
        const selectedDate = date ? new Date(date) : new Date()
        const month = (selectedDate.getMonth() + 1).toString()

        // KOLOM DINAMIS UNTUK MEMILIH ANTARA KOLOM `m1-m12`
        const monthColumn = `m${month}` as keyof typeof revenueGrosses.$inferSelect

        // VARIABLE TANGGAL UNTUK IMPORT TABEL SECARA DINAMIS
        const currMonth = format(selectedDate, 'MM')
        const isPrevMonthLastYear = currMonth === '01'
        const prevMonth = isPrevMonthLastYear ? '12' : format(subMonths(selectedDate, 1), 'MM')
        const currYear = format(selectedDate, 'yyyy')
        const prevMonthYear = isPrevMonthLastYear ? format(subYears(selectedDate, 1), 'yyyy') : format(selectedDate, 'yyyy')
        const prevYear = format(subYears(selectedDate, 1), 'yyyy')

        // TABEL DINAMIS
        const currRev = dynamicResumeRevenuePumaTable(currYear, currMonth)
        const prevMonthRev = dynamicResumeRevenuePumaTable(prevMonthYear, prevMonth)
        const prevYearCurrMonthRev = dynamicResumeRevenuePumaTable(prevYear, currMonth)

        // VARIABLE TANGGAL
        const firstDayOfCurrMonth = format(new Date(selectedDate.getFullYear(), selectedDate.getMonth(), 1), 'yyyy-MM-dd')
        const firstDayOfPrevMonth = format(subMonths(new Date(selectedDate.getFullYear(), selectedDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const firstDayOfPrevYearCurrMonth = format(subYears(new Date(selectedDate.getFullYear(), selectedDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const currDate = format(subDays(selectedDate, 2), 'yyyy-MM-dd')
        const prevDate = format(subMonths(subDays(selectedDate, 2), 1), 'yyyy-MM-dd')
        const prevYearCurrDate = format(subYears(subDays(selectedDate, 2), 1), 'yyyy-MM-dd')

        const regClassP2 = db2
            .select({
                mtdDt: currRev.mtdDt,
                rev: currRev.rev,
                regionName: sql<string>`
        CASE
          WHEN ${currRev.regionSales} IN ('MALUKU DAN PAPUA', 'PUMA', 'Puma', '13.Puma', '13. Puma') THEN 'PUMA'
        END
        `.as('regionalName'),
                kabupatenName: currRev.kabupaten,
                branchName: sql<string>`
            CASE
                WHEN ${currRev.kabupaten} IN (
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
                WHEN ${currRev.kabupaten} IN (
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
                WHEN ${currRev.kabupaten} IN (
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
                WHEN ${currRev.kabupaten} IN (
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
            WHEN ${currRev.kabupaten} IN (
                'AMBON',
                'KOTA AMBON',
                'MALUKU TENGAH',
                'SERAM BAGIAN TIMUR'
            ) THEN 'AMBON'
            WHEN ${currRev.kabupaten} IN (
                'KEPULAUAN ARU',
                'KOTA TUAL',
                'MALUKU BARAT DAYA',
                'MALUKU TENGGARA',
                'MALUKU TENGGARA BARAT',
                'KEPULAUAN TANIMBAR'
            ) THEN 'KEPULAUAN AMBON'
            WHEN ${currRev.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
            WHEN ${currRev.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
            WHEN ${currRev.kabupaten} IN (
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
            WHEN ${currRev.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
            WHEN ${currRev.kabupaten} IN (
                'FAKFAK',
                'FAK FAK',
                'KAIMANA',
                'MANOKWARI SELATAN',
                'PEGUNUNGAN ARFAK',
                'TELUK BINTUNI',
                'TELUK WONDAMA'
            ) THEN 'MANOKWARI OUTER'
            WHEN ${currRev.kabupaten} IN (
                'KOTA SORONG',
                'MAYBRAT',
                'RAJA AMPAT',
                'SORONG',
                'SORONG SELATAN',
                'TAMBRAUW'
            ) THEN 'SORONG RAJA AMPAT'
            WHEN ${currRev.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
            WHEN ${currRev.kabupaten} IN (
                'INTAN JAYA',
                'MIMIKA',
                'PUNCAK',
                'PUNCAK JAYA',
                'TIMIKA'
            ) THEN 'MIMIKA'
            WHEN ${currRev.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
            ELSE NULL
        END
            `.as('subbranchName'),
                clusterName: sql<string>`
        CASE
            WHEN ${currRev.kabupaten} IN (
                'KOTA AMBON',
                'MALUKU TENGAH',
                'SERAM BAGIAN TIMUR'
            ) THEN 'AMBON'
            WHEN ${currRev.kabupaten} IN (
                'KEPULAUAN ARU',
                'KOTA TUAL',
                'MALUKU BARAT DAYA',
                'MALUKU TENGGARA',
                'MALUKU TENGGARA BARAT',
                'KEPULAUAN TANIMBAR'
            ) THEN 'KEPULAUAN TUAL'
            WHEN ${currRev.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
            WHEN ${currRev.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
            WHEN ${currRev.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
            WHEN ${currRev.kabupaten} IN (
                'BIAK',
                'BIAK NUMFOR',
                'KEPULAUAN YAPEN',
                'SUPIORI',
                'WAROPEN'
            ) THEN 'NEW BIAK NUMFOR'
            WHEN ${currRev.kabupaten} IN (
                'JAYAWIJAYA',
                'LANNY JAYA',
                'MAMBERAMO TENGAH',
                'NDUGA',
                'PEGUNUNGAN BINTANG',
                'TOLIKARA',
                'YAHUKIMO',
                'YALIMO'
            ) THEN 'PAPUA PEGUNUNGAN'
            WHEN ${currRev.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
            WHEN ${currRev.kabupaten} IN (
                'FAKFAK',
                'FAK FAK',
                'KAIMANA',
                'MANOKWARI SELATAN',
                'PEGUNUNGAN ARFAK',
                'TELUK BINTUNI',
                'TELUK WONDAMA'
            ) THEN 'MANOKWARI OUTER'
            WHEN ${currRev.kabupaten} IN (
                'KOTA SORONG',
                'MAYBRAT',
                'RAJA AMPAT',
                'SORONG',
                'SORONG SELATAN',
                'TAMBRAUW'
            ) THEN 'NEW SORONG RAJA AMPAT'
            WHEN ${currRev.kabupaten} IN (
                'INTAN JAYA',
                'MIMIKA',
                'PUNCAK',
                'PUNCAK JAYA',
                'TIMIKA'
            ) THEN 'MIMIKA PUNCAK'
            WHEN ${currRev.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
            WHEN ${currRev.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
            ELSE NULL
        END
            `.as('clusterName'),
            })
            .from(currRev)
            .where(between(currRev.mtdDt, firstDayOfCurrMonth, currDate))
            .as('regionClassififcation')

        const kabSumsP2 = db2
            .select({
                region: sql`${regClassP2.regionName}`.as('kabRegion'),
                branch: sql<string>`${regClassP2.branchName}`.as('kabBranch'),
                subbranch: sql<string>`${regClassP2.subbranchName}`.as('kabSubbranch'),
                cluster: sql<string>`${regClassP2.clusterName}`.as('kabCluster'),
                kabupaten: regClassP2.kabupatenName,
                kabupatenRev: sql<number>`CAST(SUM(${regClassP2.rev}) AS DOUBLE PRECISION)`.as('kabupatenRev')
            })
            .from(regClassP2)
            .where(isNotNull(regClassP2.branchName))
            .groupBy(regClassP2.regionName, regClassP2.branchName, regClassP2.subbranchName, regClassP2.clusterName, regClassP2.kabupatenName)
            .as('kabSums')

        const clusSumsP2 = db2
            .select({
                region: sql`${kabSumsP2.region}`.as('clusRegion'),
                branch: sql<string>`${kabSumsP2.branch}`.as('clusBranch'),
                subbranch: sql<string>`${kabSumsP2.subbranch}`.as('clusSubbranch'),
                cluster: sql<string>`${kabSumsP2.cluster}`.as('cluster'),
                clusterRev: sql<number>`CAST(SUM(${kabSumsP2.kabupatenRev}) AS DOUBLE PRECISION)`.as('clusterRev')
            })
            .from(kabSumsP2)
            .groupBy(kabSumsP2.region, kabSumsP2.branch, kabSumsP2.subbranch, kabSumsP2.cluster)
            .as('clusSums')

        const subSumsP2 = db2
            .select({
                region: sql`${clusSumsP2.region}`.as('subRegion'),
                branch: sql<string>`${clusSumsP2.branch}`.as('subSumsBranch'),
                subbranch: sql<string>`${clusSumsP2.subbranch}`.as('subbranch'),
                subbranchRev: sql<number>`CAST(SUM(${clusSumsP2.clusterRev}) AS DOUBLE PRECISION)`.as('subbranchRev')
            })
            .from(clusSumsP2)
            .groupBy(clusSumsP2.region, clusSumsP2.branch, clusSumsP2.subbranch)
            .as('subSums')

        const branchSumsP2 = db2
            .select({
                region: sql`${subSumsP2.region}`.as('branchRegion'),
                branch: sql`${subSumsP2.branch}`.as('branch'),
                branchRev: sql<number>`CAST(SUM(${subSumsP2.subbranchRev}) AS DOUBLE PRECISION)`.as('branchRev')
            })
            .from(subSumsP2)
            .groupBy(subSumsP2.region, subSumsP2.branch)
            .as('branchSums')

        const regSumsP2 = db2
            .select({
                regionName: sql`${branchSumsP2.region}`.as('region'),
                regionalRev: sql<number>`CAST(SUM(${branchSumsP2.branchRev}) AS DOUBLE PRECISION)`.as('regionalRev')
            })
            .from(branchSumsP2)
            .groupBy(branchSumsP2.region)
            .as('regSums')

        const regClassP3 = db2
            .select({
                mtdDt: prevMonthRev.mtdDt,
                rev: prevMonthRev.rev,
                regionName: sql<string>`
        CASE
          WHEN ${prevMonthRev.regionSales} IN ('MALUKU DAN PAPUA', 'PUMA', 'Puma', '13.Puma', '13. Puma') THEN 'PUMA'
        END
        `.as('regionalName'),
                kabupatenName: prevMonthRev.kabupaten,
                branchName: sql<string>`
        CASE
            WHEN ${prevMonthRev.kabupaten} IN (
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
            WHEN ${prevMonthRev.kabupaten} IN (
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
            WHEN ${prevMonthRev.kabupaten} IN (
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
            WHEN ${prevMonthRev.kabupaten} IN (
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
            WHEN ${prevMonthRev.kabupaten} IN (
                'AMBON',
                'KOTA AMBON',
                'MALUKU TENGAH',
                'SERAM BAGIAN TIMUR'
            ) THEN 'AMBON'
            WHEN ${prevMonthRev.kabupaten} IN (
                'KEPULAUAN ARU',
                'KOTA TUAL',
                'MALUKU BARAT DAYA',
                'MALUKU TENGGARA',
                'MALUKU TENGGARA BARAT',
                'KEPULAUAN TANIMBAR'
            ) THEN 'KEPULAUAN AMBON'
            WHEN ${prevMonthRev.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
            WHEN ${prevMonthRev.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
            WHEN ${prevMonthRev.kabupaten} IN (
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
            WHEN ${prevMonthRev.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
            WHEN ${prevMonthRev.kabupaten} IN (
                'FAKFAK',
                'FAK FAK',
                'KAIMANA',
                'MANOKWARI SELATAN',
                'PEGUNUNGAN ARFAK',
                'TELUK BINTUNI',
                'TELUK WONDAMA'
            ) THEN 'MANOKWARI OUTER'
            WHEN ${prevMonthRev.kabupaten} IN (
                'KOTA SORONG',
                'MAYBRAT',
                'RAJA AMPAT',
                'SORONG',
                'SORONG SELATAN',
                'TAMBRAUW'
            ) THEN 'SORONG RAJA AMPAT'
            WHEN ${prevMonthRev.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
            WHEN ${prevMonthRev.kabupaten} IN (
                'INTAN JAYA',
                'MIMIKA',
                'PUNCAK',
                'PUNCAK JAYA',
                'TIMIKA'
            ) THEN 'MIMIKA'
            WHEN ${prevMonthRev.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
            ELSE NULL
        END
            `.as('subbranchName'),
                clusterName: sql<string>`
        CASE
            WHEN ${prevMonthRev.kabupaten} IN (
                'KOTA AMBON',
                'MALUKU TENGAH',
                'SERAM BAGIAN TIMUR'
            ) THEN 'AMBON'
            WHEN ${prevMonthRev.kabupaten} IN (
                'KEPULAUAN ARU',
                'KOTA TUAL',
                'MALUKU BARAT DAYA',
                'MALUKU TENGGARA',
                'MALUKU TENGGARA BARAT',
                'KEPULAUAN TANIMBAR'
            ) THEN 'KEPULAUAN TUAL'
            WHEN ${prevMonthRev.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
            WHEN ${prevMonthRev.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
            WHEN ${prevMonthRev.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
            WHEN ${prevMonthRev.kabupaten} IN (
                'BIAK',
                'BIAK NUMFOR',
                'KEPULAUAN YAPEN',
                'SUPIORI',
                'WAROPEN'
            ) THEN 'NEW BIAK NUMFOR'
            WHEN ${prevMonthRev.kabupaten} IN (
                'JAYAWIJAYA',
                'LANNY JAYA',
                'MAMBERAMO TENGAH',
                'NDUGA',
                'PEGUNUNGAN BINTANG',
                'TOLIKARA',
                'YAHUKIMO',
                'YALIMO'
            ) THEN 'PAPUA PEGUNUNGAN'
            WHEN ${prevMonthRev.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
            WHEN ${prevMonthRev.kabupaten} IN (
                'FAKFAK',
                'FAK FAK',
                'KAIMANA',
                'MANOKWARI SELATAN',
                'PEGUNUNGAN ARFAK',
                'TELUK BINTUNI',
                'TELUK WONDAMA'
            ) THEN 'MANOKWARI OUTER'
            WHEN ${prevMonthRev.kabupaten} IN (
                'KOTA SORONG',
                'MAYBRAT',
                'RAJA AMPAT',
                'SORONG',
                'SORONG SELATAN',
                'TAMBRAUW'
            ) THEN 'NEW SORONG RAJA AMPAT'
            WHEN ${prevMonthRev.kabupaten} IN (
                'INTAN JAYA',
                'MIMIKA',
                'PUNCAK',
                'PUNCAK JAYA',
                'TIMIKA'
            ) THEN 'MIMIKA PUNCAK'
            WHEN ${prevMonthRev.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
            WHEN ${prevMonthRev.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
            ELSE NULL
        END
            `.as('clusterName'),
            })
            .from(prevMonthRev)
            .where(between(prevMonthRev.mtdDt, firstDayOfPrevMonth, prevDate))
            .as('regionClassififcation')

        const kabSumsP3 = db2
            .select({
                region: sql`${regClassP3.regionName}`.as('kabRegion'),
                branch: sql<string>`${regClassP3.branchName}`.as('kabBranch'),
                subbranch: sql<string>`${regClassP3.subbranchName}`.as('kabSubbranch'),
                cluster: sql<string>`${regClassP3.clusterName}`.as('kabCluster'),
                kabupaten: regClassP3.kabupatenName,
                kabupatenRev: sql<number>`CAST(SUM(${regClassP3.rev}) AS DOUBLE PRECISION)`.as('kabupatenRev')
            })
            .from(regClassP3)
            .where(isNotNull(regClassP3.branchName))
            .groupBy(regClassP3.regionName, regClassP3.branchName, regClassP3.subbranchName, regClassP3.clusterName, regClassP3.kabupatenName)
            .as('kabSums')

        const clusSumsP3 = db2
            .select({
                region: sql`${kabSumsP3.region}`.as('clusRegion'),
                branch: sql<string>`${kabSumsP3.branch}`.as('clusBranch'),
                subbranch: sql<string>`${kabSumsP3.subbranch}`.as('clusSubbranch'),
                cluster: sql<string>`${kabSumsP3.cluster}`.as('cluster'),
                clusterRev: sql<number>`CAST(SUM(${kabSumsP3.kabupatenRev}) AS DOUBLE PRECISION)`.as('clusterRev')
            })
            .from(kabSumsP3)
            .groupBy(kabSumsP3.region, kabSumsP3.branch, kabSumsP3.subbranch, kabSumsP3.cluster)
            .as('clusSums')

        const subSumsP3 = db2
            .select({
                region: sql`${clusSumsP3.region}`.as('subRegion'),
                branch: sql<string>`${clusSumsP3.branch}`.as('subSumsBranch'),
                subbranch: sql<string>`${clusSumsP3.subbranch}`.as('subbranch'),
                subbranchRev: sql<number>`CAST(SUM(${clusSumsP3.clusterRev}) AS DOUBLE PRECISION)`.as('subbranchRev')
            })
            .from(clusSumsP3)
            .groupBy(clusSumsP3.region, clusSumsP3.branch, clusSumsP3.subbranch)
            .as('subSums')

        const branchSumsP3 = db2
            .select({
                region: sql`${subSumsP3.region}`.as('branchRegion'),
                branch: sql`${subSumsP3.branch}`.as('branch'),
                branchRev: sql<number>`CAST(SUM(${subSumsP3.subbranchRev}) AS DOUBLE PRECISION)`.as('branchRev')
            })
            .from(subSumsP3)
            .groupBy(subSumsP3.region, subSumsP3.branch)
            .as('branchSums')

        const regSumsP3 = db2
            .select({
                regionName: sql`${branchSumsP3.region}`.as('region'),
                regionalRev: sql<number>`CAST(SUM(${branchSumsP3.branchRev}) AS DOUBLE PRECISION)`.as('regionalRev')
            })
            .from(branchSumsP3)
            .groupBy(branchSumsP3.region)
            .as('regSums')


        const regClassP4 = db2
            .select({
                mtdDt: prevYearCurrMonthRev.mtdDt,
                rev: prevYearCurrMonthRev.rev,
                regionName: sql<string>`
        CASE
          WHEN ${prevYearCurrMonthRev.regionSales} IN ('MALUKU DAN PAPUA', 'PUMA', 'Puma', '13.Puma', '13. Puma') THEN 'PUMA'
        END
        `.as('regionalName'),
                kabupatenName: prevYearCurrMonthRev.kabupaten,
                branchName: sql<string>`
                CASE
                    WHEN ${prevYearCurrMonthRev.kabupaten} IN (
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
                    WHEN ${prevYearCurrMonthRev.kabupaten} IN (
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
                    WHEN ${prevYearCurrMonthRev.kabupaten} IN (
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
                    WHEN ${prevYearCurrMonthRev.kabupaten} IN (
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
                    WHEN ${prevYearCurrMonthRev.kabupaten} IN (
                        'AMBON',
                        'KOTA AMBON',
                        'MALUKU TENGAH',
                        'SERAM BAGIAN TIMUR'
                    ) THEN 'AMBON'
                    WHEN ${prevYearCurrMonthRev.kabupaten} IN (
                        'KEPULAUAN ARU',
                        'KOTA TUAL',
                        'MALUKU BARAT DAYA',
                        'MALUKU TENGGARA',
                        'MALUKU TENGGARA BARAT',
                        'KEPULAUAN TANIMBAR'
                    ) THEN 'KEPULAUAN AMBON'
                    WHEN ${prevYearCurrMonthRev.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
                    WHEN ${prevYearCurrMonthRev.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
                    WHEN ${prevYearCurrMonthRev.kabupaten} IN (
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
                    WHEN ${prevYearCurrMonthRev.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
                    WHEN ${prevYearCurrMonthRev.kabupaten} IN (
                        'FAKFAK',
                        'FAK FAK',
                        'KAIMANA',
                        'MANOKWARI SELATAN',
                        'PEGUNUNGAN ARFAK',
                        'TELUK BINTUNI',
                        'TELUK WONDAMA'
                    ) THEN 'MANOKWARI OUTER'
                    WHEN ${prevYearCurrMonthRev.kabupaten} IN (
                        'KOTA SORONG',
                        'MAYBRAT',
                        'RAJA AMPAT',
                        'SORONG',
                        'SORONG SELATAN',
                        'TAMBRAUW'
                    ) THEN 'SORONG RAJA AMPAT'
                    WHEN ${prevYearCurrMonthRev.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
                    WHEN ${prevYearCurrMonthRev.kabupaten} IN (
                        'INTAN JAYA',
                        'MIMIKA',
                        'PUNCAK',
                        'PUNCAK JAYA',
                        'TIMIKA'
                    ) THEN 'MIMIKA'
                    WHEN ${prevYearCurrMonthRev.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
                    ELSE NULL
                END
                `.as('subbranchName'),
                clusterName: sql<string>`
        CASE
            WHEN ${prevYearCurrMonthRev.kabupaten} IN (
                'KOTA AMBON',
                'MALUKU TENGAH',
                'SERAM BAGIAN TIMUR'
            ) THEN 'AMBON'
            WHEN ${prevYearCurrMonthRev.kabupaten} IN (
                'KEPULAUAN ARU',
                'KOTA TUAL',
                'MALUKU BARAT DAYA',
                'MALUKU TENGGARA',
                'MALUKU TENGGARA BARAT',
                'KEPULAUAN TANIMBAR'
            ) THEN 'KEPULAUAN TUAL'
            WHEN ${prevYearCurrMonthRev.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
            WHEN ${prevYearCurrMonthRev.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
            WHEN ${prevYearCurrMonthRev.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
            WHEN ${prevYearCurrMonthRev.kabupaten} IN (
                'BIAK',
                'BIAK NUMFOR',
                'KEPULAUAN YAPEN',
                'SUPIORI',
                'WAROPEN'
            ) THEN 'NEW BIAK NUMFOR'
            WHEN ${prevYearCurrMonthRev.kabupaten} IN (
                'JAYAWIJAYA',
                'LANNY JAYA',
                'MAMBERAMO TENGAH',
                'NDUGA',
                'PEGUNUNGAN BINTANG',
                'TOLIKARA',
                'YAHUKIMO',
                'YALIMO'
            ) THEN 'PAPUA PEGUNUNGAN'
            WHEN ${prevYearCurrMonthRev.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
            WHEN ${prevYearCurrMonthRev.kabupaten} IN (
                'FAKFAK',
                'FAK FAK',
                'KAIMANA',
                'MANOKWARI SELATAN',
                'PEGUNUNGAN ARFAK',
                'TELUK BINTUNI',
                'TELUK WONDAMA'
            ) THEN 'MANOKWARI OUTER'
            WHEN ${prevYearCurrMonthRev.kabupaten} IN (
                'KOTA SORONG',
                'MAYBRAT',
                'RAJA AMPAT',
                'SORONG',
                'SORONG SELATAN',
                'TAMBRAUW'
            ) THEN 'NEW SORONG RAJA AMPAT'
            WHEN ${prevYearCurrMonthRev.kabupaten} IN (
                'INTAN JAYA',
                'MIMIKA',
                'PUNCAK',
                'PUNCAK JAYA',
                'TIMIKA'
            ) THEN 'MIMIKA PUNCAK'
            WHEN ${prevYearCurrMonthRev.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
            WHEN ${prevYearCurrMonthRev.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
            ELSE NULL
        END
            `.as('clusterName'),
            })
            .from(prevYearCurrMonthRev)
            .where(between(prevYearCurrMonthRev.mtdDt, firstDayOfPrevYearCurrMonth, prevYearCurrDate))
            .as('regionClassififcation')

        const kabSumsP4 = db2
            .select({
                region: sql`${regClassP4.regionName}`.as('kabRegion'),
                branch: sql<string>`${regClassP4.branchName}`.as('kabBranch'),
                subbranch: sql<string>`${regClassP4.subbranchName}`.as('kabSubbranch'),
                cluster: sql<string>`${regClassP4.clusterName}`.as('kabCluster'),
                kabupaten: regClassP4.kabupatenName,
                kabupatenRev: sql<number>`SUM(${regClassP4.rev})`.as('kabupatenRev')
            })
            .from(regClassP4)
            .where(isNotNull(regClassP4.branchName))
            .groupBy(regClassP4.regionName, regClassP4.branchName, regClassP4.subbranchName, regClassP4.clusterName, regClassP4.kabupatenName)
            .as('kabSums')

        const clusSumsP4 = db2
            .select({
                region: sql`${kabSumsP4.region}`.as('clusRegion'),
                branch: sql<string>`${kabSumsP4.branch}`.as('clusBranch'),
                subbranch: sql<string>`${kabSumsP4.subbranch}`.as('clusSubbranch'),
                cluster: sql<string>`${kabSumsP4.cluster}`.as('cluster'),
                clusterRev: sql<number>`SUM(${kabSumsP4.kabupatenRev})`.as('clusterRev')
            })
            .from(kabSumsP4)
            .groupBy(kabSumsP4.region, kabSumsP4.branch, kabSumsP4.subbranch, kabSumsP4.cluster)
            .as('clusSums')

        const subSumsP4 = db2
            .select({
                region: sql`${clusSumsP4.region}`.as('subRegion'),
                branch: sql<string>`${clusSumsP4.branch}`.as('subSumsBranch'),
                subbranch: sql<string>`${clusSumsP4.subbranch}`.as('subbranch'),
                subbranchRev: sql<number>`SUM(${clusSumsP4.clusterRev})`.as('subbranchRev')
            })
            .from(clusSumsP4)
            .groupBy(clusSumsP4.region, clusSumsP4.branch, clusSumsP4.subbranch)
            .as('subSums')

        const branchSumsP4 = db2
            .select({
                region: sql`${subSumsP4.region}`.as('branchRegion'),
                branch: sql`${subSumsP4.branch}`.as('branch'),
                branchRev: sql<number>`SUM(${subSumsP4.subbranchRev})`.as('branchRev')
            })
            .from(subSumsP4)
            .groupBy(subSumsP4.region, subSumsP4.branch)
            .as('branchSums')

        const regSumsP4 = db2
            .select({
                regionName: sql`${branchSumsP4.region}`.as('region'),
                regionalRev: sql<number>`CAST(SUM(${branchSumsP4.branchRev}) AS DOUBLE PRECISION)`.as('regionalRev')
            })
            .from(branchSumsP4)
            .groupBy(branchSumsP4.region)
            .as('regSums')

        console.log({ monthColumn });

        // QUERY UNTUK TARGET BULAN INI
        const p1 = db
            .select({
                regionalName: regionals.regional,
                branchName: branches.branchNew,
                subbranchName: subbranches.subbranchNew,
                clusterName: clusters.cluster,
                kabupatenName: kabupatens.kabupaten,
                currMonthTargetRev: sql<number>`CAST(SUM(${revenueGrosses[monthColumn]}) AS DOUBLE PRECISION)`.as('currMonthTargetRev')
            })
            .from(regionals)
            .leftJoin(branches, eq(regionals.id, branches.regionalId), { useIndex: index('idx_regional_id').on(branches.regionalId).using('btree') })
            .leftJoin(subbranches, eq(branches.id, subbranches.branchId), { useIndex: index('idx_branch_id').on(subbranches.branchId).using('btree') })
            .leftJoin(clusters, eq(subbranches.id, clusters.subbranchId), { useIndex: index('idx_subbranch_id').on(clusters.subbranchId).using('btree') })
            .leftJoin(kabupatens, eq(clusters.id, kabupatens.clusterId), { useIndex: index('idx_cluster_id').on(kabupatens.clusterId).using('btree') })
            .innerJoin(revenueGrosses, eq(kabupatens.id, revenueGrosses.kabupatenId), { useIndex: index('idx_kabupaten_id').on(revenueGrosses.kabupatenId).using('btree') })
            .groupBy(
                regionals.regional,
                branches.branchNew,
                subbranches.subbranchNew,
                clusters.cluster,
                kabupatens.kabupaten
            )
            .orderBy(regionals.regional, branches.branchNew, subbranches.subbranchNew, clusters.cluster, kabupatens.kabupaten)
            .prepare()

        // QUERY UNTUK PENDAPATAN BULAN INI
        const p2 = db2
            .select({
                region: sql<string>`${kabSumsP2.region}`.as('region'),
                branch: sql<string>`${kabSumsP2.branch}`.as('branch'), // Keep only one branchName
                subbranch: kabSumsP2.subbranch,
                cluster: kabSumsP2.cluster,
                kabupaten: sql<string>`${kabSumsP2.kabupaten}`.as('kabupaten'),
                currMonthKabupatenRev: kabSumsP2.kabupatenRev,
                currMonthClusterRev: clusSumsP2.clusterRev,
                currMonthSubbranchRev: subSumsP2.subbranchRev,
                currMonthBranchRev: branchSumsP2.branchRev,
                currMonthRegionalRev: regSumsP2.regionalRev
            })
            .from(kabSumsP2)
            .innerJoin(clusSumsP2, and(
                and(eq(kabSumsP2.region, clusSumsP2.region), eq(kabSumsP2.branch, clusSumsP2.branch)),
                and(eq(kabSumsP2.subbranch, clusSumsP2.subbranch), eq(kabSumsP2.cluster, clusSumsP2.cluster))
            ))
            .innerJoin(subSumsP2, and(
                eq(kabSumsP2.region, subSumsP2.region),
                and(eq(kabSumsP2.branch, subSumsP2.branch), eq(kabSumsP2.subbranch, subSumsP2.subbranch))
            ))
            .innerJoin(branchSumsP2, and(
                eq(kabSumsP2.region, branchSumsP2.region),
                eq(kabSumsP2.branch, branchSumsP2.branch)
            ))
            .innerJoin(regSumsP2, eq(kabSumsP2.region, regSumsP2.regionName))
            .orderBy(kabSumsP2.region, kabSumsP2.branch, kabSumsP2.subbranch, kabSumsP2.cluster, kabSumsP2.kabupaten)
            .prepare()

        // QUERY UNTUK PENDAPATAN BULAN SEBELUMNYA
        const p3 = db2
            .select({
                region: sql<string>`${kabSumsP3.region}`.as('region'),
                branch: sql<string>`${kabSumsP3.branch}`.as('branch'), // Keep only one branchName
                subbranch: kabSumsP3.subbranch,
                cluster: kabSumsP3.cluster,
                kabupaten: sql<string>`${kabSumsP3.kabupaten}`.as('kabupaten'),
                prevMonthKabupatenRev: kabSumsP3.kabupatenRev,
                prevMonthClusterRev: clusSumsP3.clusterRev,
                prevMonthSubbranchRev: subSumsP3.subbranchRev,
                prevMonthBranchRev: branchSumsP3.branchRev,
                prevMonthRegionalRev: regSumsP3.regionalRev
            })
            .from(kabSumsP3)
            .innerJoin(clusSumsP3, and(
                and(eq(kabSumsP3.region, clusSumsP3.region), eq(kabSumsP3.branch, clusSumsP3.branch)),
                and(eq(kabSumsP3.subbranch, clusSumsP3.subbranch), eq(kabSumsP3.cluster, clusSumsP3.cluster))
            ))
            .innerJoin(subSumsP3, and(
                eq(kabSumsP3.region, subSumsP3.region),
                and(eq(kabSumsP3.branch, subSumsP3.branch), eq(kabSumsP3.subbranch, subSumsP3.subbranch))
            ))
            .innerJoin(branchSumsP3, and(eq(kabSumsP3.region, branchSumsP3.region), eq(kabSumsP3.branch, branchSumsP3.branch)))
            .innerJoin(regSumsP3, eq(kabSumsP3.region, regSumsP3.regionName))
            .orderBy(kabSumsP3.region, kabSumsP3.branch, kabSumsP3.subbranch, kabSumsP3.cluster, kabSumsP3.kabupaten)
            .prepare()

        // QUERY UNTUK MENDAPAT PREV YEAR CURR MONTH REVENUE
        const p4 = db2
            .select({
                region: sql<string>`${kabSumsP4.region}`.as('region'),
                branch: sql<string>`${kabSumsP4.branch}`.as('branch'), // Keep only one branchName
                subbranch: kabSumsP4.subbranch,
                cluster: kabSumsP4.cluster,
                kabupaten: sql<string>`${kabSumsP4.kabupaten}`.as('kabupaten'),
                prevYearCurrMonthKabupatenRev: kabSumsP4.kabupatenRev,
                prevYearCurrMonthClusterRev: clusSumsP4.clusterRev,
                prevYearCurrMonthSubbranchRev: subSumsP4.subbranchRev,
                prevYearCurrMonthBranchRev: branchSumsP4.branchRev,
                prevYearCurrMonthRegionalRev: regSumsP4.regionalRev
            })
            .from(kabSumsP4)
            .innerJoin(clusSumsP4, and(
                and(eq(kabSumsP4.region, clusSumsP4.region), eq(kabSumsP4.branch, clusSumsP4.branch)),
                and(eq(kabSumsP4.subbranch, clusSumsP4.subbranch), eq(kabSumsP4.cluster, clusSumsP4.cluster))
            ))
            .innerJoin(subSumsP4, and(
                eq(kabSumsP4.region, subSumsP4.region),
                and(eq(kabSumsP4.branch, subSumsP4.branch), eq(kabSumsP4.subbranch, subSumsP4.subbranch))
            ))
            .innerJoin(branchSumsP4, and(eq(kabSumsP4.region, branchSumsP4.region), eq(kabSumsP4.branch, branchSumsP4.branch)))
            .innerJoin(regSumsP4, eq(kabSumsP4.region, regSumsP4.regionName))
            .orderBy(kabSumsP4.region, kabSumsP4.branch, kabSumsP4.subbranch, kabSumsP4.cluster, kabSumsP4.kabupaten)
            .prepare()

        const [targetRevenue, currMonthRevenue, prevMonthRevenue, prevYearCurrMonthRevenue] = await Promise.all([
            p1.execute(),
            p2.execute(),
            p3.execute(),
            p4.execute()
        ]);

        const regionalsMap = new Map();

        targetRevenue.forEach((row) => {
            const regionalName = row.regionalName;
            const branchName = row.branchName;
            const subbranchName = row.subbranchName;
            const clusterName = row.clusterName;
            const kabupatenName = row.kabupatenName;

            const regional = regionalsMap.get(regionalName) || regionalsMap.set(regionalName, {
                name: regionalName,
                currMonthRevenue: 0,
                currMonthTarget: 0,
                prevMonthRevenue: 0,
                prevYearCurrMonthReveneu: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthTarget += Number(row.currMonthTargetRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthReveneu: 0,
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
                    prevYearCurrMonthReveneu: 0,
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
                    prevYearCurrMonthReveneu: 0,
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
                    prevYearCurrMonthReveneu: 0
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
                prevYearCurrMonthReveneu: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthRevenue = Number(row.currMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthReveneu: 0,
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
                    prevYearCurrMonthReveneu: 0,
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
                    prevYearCurrMonthReveneu: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.currMonthRevenue = Number(row.currMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: Number(row.currMonthKabupatenRev),
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthReveneu: 0
                }), cluster.kabupatens.get(kabupatenName));
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
                prevYearCurrMonthReveneu: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevMonthRevenue = Number(row.prevMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthReveneu: 0,
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
                    prevYearCurrMonthReveneu: 0,
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
                    prevYearCurrMonthReveneu: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevMonthRevenue = Number(row.prevMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: Number(row.prevMonthKabupatenRev),
                    prevYearCurrMonthReveneu: 0
                }), cluster.kabupatens.get(kabupatenName));
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
                prevYearCurrMonthReveneu: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevYearCurrMonthReveneu = Number(row.prevYearCurrMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthReveneu: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.prevYearCurrMonthReveneu = Number(row.prevYearCurrMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthReveneu: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.prevYearCurrMonthReveneu = Number(row.prevYearCurrMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthReveneu: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevYearCurrMonthReveneu = Number(row.prevYearCurrMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthReveneu: Number(row.prevYearCurrMonthKabupatenRev)
                }), cluster.kabupatens.get(kabupatenName));
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
    .get('/revenue-byu', async ({ db, db3, query }) => {
        const { branch, cluster, subbranch, kabupaten, date } = query
        const selectedDate = date ? new Date(date) : new Date()
        const month = (selectedDate.getMonth() + 1).toString()

        // KOLOM DINAMIS UNTUK MEMILIH ANTARA KOLOM `m1-m12`
        const monthColumn = `m${month}` as keyof typeof revenueByu.$inferSelect

        // VARIABLE TANGGAL UNTUK IMPORT TABEL SECARA DINAMIS
        const currMonth = format(selectedDate, 'MM')
        const isPrevMonthLastYear = currMonth === '01'
        const prevMonth = isPrevMonthLastYear ? '12' : format(subMonths(selectedDate, 1), 'MM')
        const currYear = format(selectedDate, 'yyyy')
        const prevMonthYear = isPrevMonthLastYear ? format(subYears(selectedDate, 1), 'yyyy') : format(selectedDate, 'yyyy')
        const prevYear = format(subYears(selectedDate, 1), 'yyyy')

        const currRevByu = dynamicByuTable(currYear, currMonth)
        const prevMonthRevByu = dynamicByuTable(prevMonthYear, prevMonth)
        const prevYearCurrMonthRevByu = dynamicByuTable(prevYear, currMonth)

        // VARIABLE TANGGAL
        const firstDayOfCurrMonth = format(new Date(selectedDate.getFullYear(), selectedDate.getMonth(), 1), 'yyyy-MM-dd')
        const firstDayOfPrevMonth = format(subMonths(new Date(selectedDate.getFullYear(), selectedDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const firstDayOfPrevYearCurrMonth = format(subYears(new Date(selectedDate.getFullYear(), selectedDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const currDate = format(subDays(selectedDate, 2), 'yyyy-MM-dd')
        const prevDate = format(subMonths(subDays(selectedDate, 2), 1), 'yyyy-MM-dd')
        const prevYearCurrDate = format(subYears(subDays(selectedDate, 2), 1), 'yyyy-MM-dd')

        const regClassP2 = db3.select({
            msisdn: currRevByu.msisdn,
            periodde: currRevByu.periode,
            eventDate: currRevByu.eventDate,
            rev: currRevByu.rev,
            regionName: currRevByu.regionSales,
            kabupatenName: currRevByu.kabupaten,
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
        })
            .from(currRevByu)
            .where(between(currRevByu.eventDate, firstDayOfCurrMonth, currDate))
            .as('regionClassififcation')

        const kabSumsP2 = db3
            .select({
                region: regClassP2.regionName,
                branch: sql<string>`${regClassP2.branchName}`.as('kabBranch'),
                subbranch: sql<string>`${regClassP2.subbranchName}`.as('kabSubbranch'),
                cluster: sql<string>`${regClassP2.clusterName}`.as('kabCluster'),
                kabupaten: regClassP2.kabupatenName,
                kabupatenRev: sql<number>`CAST(SUM(${regClassP2.rev}) AS DOUBLE PRECISION)`.as('kabupatenRev')
            })
            .from(regClassP2)
            .where(isNotNull(regClassP2.branchName))
            .groupBy(regClassP2.regionName, regClassP2.branchName, regClassP2.subbranchName, regClassP2.clusterName, regClassP2.kabupatenName)
            .as('kabSums')

        const clusSumsP2 = db3
            .select({
                region: kabSumsP2.region,
                branch: sql<string>`${kabSumsP2.branch}`.as('clusBranch'),
                subbranch: sql<string>`${kabSumsP2.subbranch}`.as('clusSubbranch'),
                cluster: sql<string>`${kabSumsP2.cluster}`.as('cluster'),
                clusterRev: sql<number>`CAST(SUM(${kabSumsP2.kabupatenRev}) AS DOUBLE PRECISION)`.as('clusterRev')
            })
            .from(kabSumsP2)
            .groupBy(kabSumsP2.region, kabSumsP2.branch, kabSumsP2.subbranch, kabSumsP2.cluster)
            .as('clusSums')

        const subSumsP2 = db3
            .select({
                region: clusSumsP2.region,
                branch: sql<string>`${clusSumsP2.branch}`.as('subSumsBranch'),
                subbranch: sql<string>`${clusSumsP2.subbranch}`.as('subbranch'),
                subbranchRev: sql<number>`CAST(SUM(${clusSumsP2.clusterRev}) AS DOUBLE PRECISION)`.as('subbranchRev')
            })
            .from(clusSumsP2)
            .groupBy(clusSumsP2.region, clusSumsP2.branch, clusSumsP2.subbranch)
            .as('subSums')

        const branchSumsP2 = db3
            .select({
                region: subSumsP2.region,
                branch: sql`${subSumsP2.branch}`.as('branch'),
                branchRev: sql<number>`CAST(SUM(${subSumsP2.subbranchRev}) AS DOUBLE PRECISION)`.as('branchRev')
            })
            .from(subSumsP2)
            .groupBy(subSumsP2.region, subSumsP2.branch)
            .as('branchSums')

        const regSumsP2 = db3
            .select({
                regionName: branchSumsP2.region,
                regionalRev: sql<number>`CAST(SUM(${branchSumsP2.branchRev}) AS DOUBLE PRECISION)`.as('regionalRev')
            })
            .from(branchSumsP2)
            .groupBy(branchSumsP2.region)
            .as('regSums')

        const regClassP3 = db3
            .select({
                msisdn: prevMonthRevByu.msisdn,
                periodde: prevMonthRevByu.periode,
                eventDate: prevMonthRevByu.eventDate,
                rev: prevMonthRevByu.rev,
                regionName: prevMonthRevByu.regionSales,
                kabupatenName: prevMonthRevByu.kabupaten,
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
            })
            .from(prevMonthRevByu)
            .where(between(prevMonthRevByu.eventDate, firstDayOfPrevMonth, prevDate))
            .as('regionClassififcation')

        const kabSumsP3 = db3
            .select({
                region: regClassP3.regionName,
                branch: sql<string>`${regClassP3.branchName}`.as('kabBranch'),
                subbranch: sql<string>`${regClassP3.subbranchName}`.as('kabSubbranch'),
                cluster: sql<string>`${regClassP3.clusterName}`.as('kabCluster'),
                kabupaten: regClassP3.kabupatenName,
                kabupatenRev: sql<number>`CAST(SUM(${regClassP3.rev}) AS DOUBLE PRECISION)`.as('kabupatenRev')
            })
            .from(regClassP3)
            .where(isNotNull(regClassP3.branchName))
            .groupBy(regClassP3.regionName, regClassP3.branchName, regClassP3.subbranchName, regClassP3.clusterName, regClassP3.kabupatenName)
            .as('kabSums')

        const clusSumsP3 = db3
            .select({
                region: kabSumsP3.region,
                branch: sql<string>`${kabSumsP3.branch}`.as('clusBranch'),
                subbranch: sql<string>`${kabSumsP3.subbranch}`.as('clusSubbranch'),
                cluster: sql<string>`${kabSumsP3.cluster}`.as('cluster'),
                clusterRev: sql<number>`CAST(SUM(${kabSumsP3.kabupatenRev}) AS DOUBLE PRECISION)`.as('clusterRev')
            })
            .from(kabSumsP3)
            .groupBy(kabSumsP3.region, kabSumsP3.branch, kabSumsP3.subbranch, kabSumsP3.cluster)
            .as('clusSums')

        const subSumsP3 = db3
            .select({
                region: clusSumsP3.region,
                branch: sql<string>`${clusSumsP3.branch}`.as('subSumsBranch'),
                subbranch: sql<string>`${clusSumsP3.subbranch}`.as('subbranch'),
                subbranchRev: sql<number>`CAST(SUM(${clusSumsP3.clusterRev}) AS DOUBLE PRECISION)`.as('subbranchRev')
            })
            .from(clusSumsP3)
            .groupBy(clusSumsP3.region, clusSumsP3.branch, clusSumsP3.subbranch)
            .as('subSums')

        const branchSumsP3 = db3
            .select({
                region: subSumsP3.region,
                branch: sql`${subSumsP3.branch}`.as('branch'),
                branchRev: sql<number>`CAST(SUM(${subSumsP3.subbranchRev}) AS DOUBLE PRECISION)`.as('branchRev')
            })
            .from(subSumsP3)
            .groupBy(subSumsP3.region, subSumsP3.branch)
            .as('branchSums')

        const regSumsP3 = db3
            .select({
                regionName: branchSumsP3.region,
                regionalRev: sql<number>`CAST(SUM(${branchSumsP3.branchRev}) AS DOUBLE PRECISION)`.as('regionalRev')
            })
            .from(branchSumsP3)
            .groupBy(branchSumsP3.region)
            .as('regSums')


        const regClassP4 = db3
            .select({
                msisdn: prevYearCurrMonthRevByu.msisdn,
                periodde: prevYearCurrMonthRevByu.periode,
                eventDate: prevYearCurrMonthRevByu.eventDate,
                rev: prevYearCurrMonthRevByu.rev,
                regionName: prevYearCurrMonthRevByu.regionSales,
                kabupatenName: prevYearCurrMonthRevByu.kabupaten,
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
            })
            .from(prevYearCurrMonthRevByu)
            .where(between(prevYearCurrMonthRevByu.eventDate, firstDayOfPrevYearCurrMonth, prevYearCurrDate))
            .as('regionClassififcation')

        const kabSumsP4 = db3
            .select({
                region: regClassP4.regionName,
                branch: sql<string>`${regClassP4.branchName}`.as('kabBranch'),
                subbranch: sql<string>`${regClassP4.subbranchName}`.as('kabSubbranch'),
                cluster: sql<string>`${regClassP4.clusterName}`.as('kabCluster'),
                kabupaten: regClassP4.kabupatenName,
                kabupatenRev: sql<number>`SUM(${regClassP4.rev})`.as('kabupatenRev')
            })
            .from(regClassP4)
            .where(isNotNull(regClassP4.branchName))
            .groupBy(regClassP4.regionName, regClassP4.branchName, regClassP4.subbranchName, regClassP4.clusterName, regClassP4.kabupatenName)
            .as('kabSums')

        const clusSumsP4 = db3
            .select({
                region: kabSumsP4.region,
                branch: sql<string>`${kabSumsP4.branch}`.as('clusBranch'),
                subbranch: sql<string>`${kabSumsP4.subbranch}`.as('clusSubbranch'),
                cluster: sql<string>`${kabSumsP4.cluster}`.as('cluster'),
                clusterRev: sql<number>`SUM(${kabSumsP4.kabupatenRev})`.as('clusterRev')
            })
            .from(kabSumsP4)
            .groupBy(kabSumsP4.region, kabSumsP4.branch, kabSumsP4.subbranch, kabSumsP4.cluster)
            .as('clusSums')

        const subSumsP4 = db3
            .select({
                region: clusSumsP4.region,
                branch: sql<string>`${clusSumsP4.branch}`.as('subSumsBranch'),
                subbranch: sql<string>`${clusSumsP4.subbranch}`.as('subbranch'),
                subbranchRev: sql<number>`SUM(${clusSumsP4.clusterRev})`.as('subbranchRev')
            })
            .from(clusSumsP4)
            .groupBy(clusSumsP4.region, clusSumsP4.branch, clusSumsP4.subbranch)
            .as('subSums')

        const branchSumsP4 = db3
            .select({
                region: subSumsP4.region,
                branch: sql`${subSumsP4.branch}`.as('branch'),
                branchRev: sql<number>`SUM(${subSumsP4.subbranchRev})`.as('branchRev')
            })
            .from(subSumsP4)
            .groupBy(subSumsP4.region, subSumsP4.branch)
            .as('branchSums')

        const regSumsP4 = db3
            .select({
                regionName: branchSumsP4.region,
                regionalRev: sql<number>`CAST(SUM(${branchSumsP4.branchRev}) AS DOUBLE PRECISION)`.as('regionalRev')
            })
            .from(branchSumsP4)
            .groupBy(branchSumsP4.region)
            .as('regSums')


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
                region: sql<string>`${kabSumsP2.region}`.as('region'),
                branch: sql<string>`${kabSumsP2.branch}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${kabSumsP2.subbranch}`.as('subbranch'),
                cluster: sql<string>`${kabSumsP2.cluster}`.as('cluster'),
                kabupaten: sql<string>`${kabSumsP2.kabupaten}`.as('kabupaten'),
                currMonthKabupatenRev: kabSumsP2.kabupatenRev,
                currMonthClusterRev: clusSumsP2.clusterRev,
                currMonthSubbranchRev: subSumsP2.subbranchRev,
                currMonthBranchRev: branchSumsP2.branchRev,
                currMonthRegionalRev: regSumsP2.regionalRev
            })
            .from(kabSumsP2)
            .innerJoin(clusSumsP2, and(
                and(eq(kabSumsP2.region, clusSumsP2.region), eq(kabSumsP2.branch, clusSumsP2.branch)),
                and(eq(kabSumsP2.subbranch, clusSumsP2.subbranch), eq(kabSumsP2.cluster, clusSumsP2.cluster))
            ))
            .innerJoin(subSumsP2, and(
                eq(kabSumsP2.region, subSumsP2.region),
                and(eq(kabSumsP2.branch, subSumsP2.branch), eq(kabSumsP2.subbranch, subSumsP2.subbranch))
            ))
            .innerJoin(branchSumsP2, and(eq(kabSumsP2.region, branchSumsP2.region), eq(kabSumsP2.branch, branchSumsP2.branch)))
            .innerJoin(regSumsP2, eq(kabSumsP2.region, regSumsP2.regionName))
            .orderBy(kabSumsP2.region, kabSumsP2.branch, kabSumsP2.subbranch, kabSumsP2.cluster, kabSumsP2.kabupaten)
            .prepare()

        // QUERY UNTUK MENDAPAT PREV MONTH REVENUE
        const p3 = db3
            .select({
                region: sql<string>`${kabSumsP3.region}`.as('region'),
                branch: sql<string>`${kabSumsP3.branch}`.as('branch'), // Keep only one branchName
                subbranch: kabSumsP3.subbranch,
                cluster: kabSumsP3.cluster,
                kabupaten: sql<string>`${kabSumsP3.kabupaten}`.as('kabupaten'),
                prevMonthKabupatenRev: kabSumsP3.kabupatenRev,
                prevMonthClusterRev: clusSumsP3.clusterRev,
                prevMonthSubbranchRev: subSumsP3.subbranchRev,
                prevMonthBranchRev: branchSumsP3.branchRev,
                prevMonthRegionalRev: regSumsP3.regionalRev
            })
            .from(kabSumsP3)
            .innerJoin(clusSumsP3, and(
                and(eq(kabSumsP3.region, clusSumsP3.region), eq(kabSumsP3.branch, clusSumsP3.branch)),
                and(eq(kabSumsP3.subbranch, clusSumsP3.subbranch), eq(kabSumsP3.cluster, clusSumsP3.cluster))
            ))
            .innerJoin(subSumsP3, and(
                eq(kabSumsP3.region, subSumsP3.region),
                and(eq(kabSumsP3.branch, subSumsP3.branch), eq(kabSumsP3.subbranch, subSumsP3.subbranch))
            ))
            .innerJoin(branchSumsP3, and(eq(kabSumsP3.region, branchSumsP3.region), eq(kabSumsP3.branch, branchSumsP3.branch)))
            .innerJoin(regSumsP3, eq(kabSumsP3.region, regSumsP3.regionName))
            .orderBy(kabSumsP3.region, kabSumsP3.branch, kabSumsP3.subbranch, kabSumsP3.cluster, kabSumsP3.kabupaten)
            .prepare()

        // QUERY UNTUK MENDAPAT PREV YEAR CURR MONTH REVENUE
        const p4 = db3
            .select({
                region: sql<string>`${kabSumsP4.region}`.as('region'),
                branch: sql<string>`${kabSumsP4.branch}`.as('branch'), // Keep only one branchName
                subbranch: kabSumsP4.subbranch,
                cluster: kabSumsP4.cluster,
                kabupaten: sql<string>`${kabSumsP4.kabupaten}`.as('kabupaten'),
                prevYearCurrMonthKabupatenRev: kabSumsP4.kabupatenRev,
                prevYearCurrMonthClusterRev: clusSumsP4.clusterRev,
                prevYearCurrMonthSubbranchRev: subSumsP4.subbranchRev,
                prevYearCurrMonthBranchRev: branchSumsP4.branchRev,
                prevYearCurrMonthRegionalRev: regSumsP4.regionalRev
            })
            .from(kabSumsP4)
            .innerJoin(clusSumsP4, and(
                and(eq(kabSumsP4.region, clusSumsP4.region), eq(kabSumsP4.branch, clusSumsP4.branch)),
                and(eq(kabSumsP4.subbranch, clusSumsP4.subbranch), eq(kabSumsP4.cluster, clusSumsP4.cluster))
            ))
            .innerJoin(subSumsP4, and(
                eq(kabSumsP4.region, subSumsP4.region),
                and(eq(kabSumsP4.branch, subSumsP4.branch), eq(kabSumsP4.subbranch, subSumsP4.subbranch))
            ))
            .innerJoin(branchSumsP4, and(eq(kabSumsP4.region, branchSumsP4.region), eq(kabSumsP4.branch, branchSumsP4.branch)))
            .innerJoin(regSumsP4, eq(kabSumsP4.region, regSumsP4.regionName))
            .orderBy(kabSumsP4.region, kabSumsP4.branch, kabSumsP4.subbranch, kabSumsP4.cluster, kabSumsP4.kabupaten)
            .prepare()

        // QUERY UNTUK YtD 2025

        const [targetRevenue, currMonthRevenue, prevMonthRevenue, prevYearRevenue] = await Promise.all([
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
                prevYearCurrMonthReveneu: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthTarget += Number(row.currMonthTargetRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthReveneu: 0,
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
                    prevYearCurrMonthReveneu: 0,
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
                    prevYearCurrMonthReveneu: 0,
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
                    prevYearCurrMonthReveneu: 0
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
                prevYearCurrMonthReveneu: 0,
                branches: new Map()
            }).get(regionalName);
            regional.currMonthRevenue = Number(row.currMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthReveneu: 0,
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
                    prevYearCurrMonthReveneu: 0,
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
                    prevYearCurrMonthReveneu: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.currMonthRevenue = Number(row.currMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: Number(row.currMonthKabupatenRev),
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthReveneu: 0
                }), cluster.kabupatens.get(kabupatenName));
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
                prevYearCurrMonthReveneu: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevMonthRevenue = Number(row.prevMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthReveneu: 0,
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
                    prevYearCurrMonthReveneu: 0,
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
                    prevYearCurrMonthReveneu: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevMonthRevenue = Number(row.prevMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: Number(row.prevMonthKabupatenRev),
                    prevYearCurrMonthReveneu: 0
                }), cluster.kabupatens.get(kabupatenName));
        })

        prevYearRevenue.forEach((row) => {
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
                prevYearCurrMonthReveneu: 0,
                branches: new Map()
            }).get(regionalName);
            regional.prevYearCurrMonthReveneu = Number(row.prevYearCurrMonthRegionalRev)

            const branch = regional.branches.get(branchName) ||
                (regional.branches.set(branchName, {
                    name: branchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthReveneu: 0,
                    subbranches: new Map()
                }), regional.branches.get(branchName));  // Get the newly set value
            branch.prevYearCurrMonthReveneu = Number(row.prevYearCurrMonthBranchRev)

            // Initialize subbranch if it doesn't exist
            const subbranch = branch.subbranches.get(subbranchName) ||
                (branch.subbranches.set(subbranchName, {
                    name: subbranchName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthReveneu: 0,
                    clusters: new Map()
                }), branch.subbranches.get(subbranchName));
            subbranch.prevYearCurrMonthReveneu = Number(row.prevYearCurrMonthSubbranchRev)

            // Initialize cluster if it doesn't exist
            const cluster = subbranch.clusters.get(clusterName) ||
                (subbranch.clusters.set(clusterName, {
                    name: clusterName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthReveneu: 0,
                    kabupatens: new Map()
                }), subbranch.clusters.get(clusterName));
            cluster.prevYearCurrMonthReveneu = Number(row.prevYearCurrMonthClusterRev)

            // Initialize kabupaten if it doesn't exist
            cluster.kabupatens.get(kabupatenName) ||
                (cluster.kabupatens.set(kabupatenName, {
                    name: kabupatenName,
                    currMonthRevenue: 0,
                    currMonthTarget: 0,
                    prevMonthRevenue: 0,
                    prevYearCurrMonthReveneu: Number(row.prevYearCurrMonthKabupatenRev)
                }), cluster.kabupatens.get(kabupatenName));
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
    .get('/revenue-grosses', async ({ db, db2, db3, query }) => {
        const { date, branch, cluster, kabupaten, subbranch } = query
        const selectedDate = date ? new Date(date) : new Date()
        const month = (selectedDate.getMonth() + 1).toString()

        // KOLOM DINAMIS UNTUK MEMILIH ANTARA KOLOM `m1-m12`
        const monthColumn = `m${month}` as keyof typeof revenueGrosses.$inferSelect

        // VARIABLE TANGGAL UNTUK IMPORT TABEL SECARA DINAMIS
        const currMonth = format(selectedDate, 'MM')
        const isPrevMonthLastYear = currMonth === '01'
        const prevMonth = isPrevMonthLastYear ? '12' : format(subMonths(selectedDate, 1), 'MM')
        const currYear = format(selectedDate, 'yyyy')
        const prevMonthYear = isPrevMonthLastYear ? format(subYears(selectedDate, 1), 'yyyy') : format(selectedDate, 'yyyy')
        const prevYear = format(subYears(selectedDate, 1), 'yyyy')

        // TABEL DINAMIS
        const currRev = dynamicResumeRevenuePumaTable(currYear, currMonth)
        const prevMonthRev = dynamicResumeRevenuePumaTable(prevMonthYear, prevMonth)
        const prevYearCurrMonthRev = dynamicResumeRevenuePumaTable(prevYear, currMonth)

        // VARIABLE TANGGAL
        const firstDayOfCurrMonth = format(new Date(selectedDate.getFullYear(), selectedDate.getMonth(), 1), 'yyyy-MM-dd')
        const firstDayOfPrevMonth = format(subMonths(new Date(selectedDate.getFullYear(), selectedDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const firstDayOfPrevYearCurrMonth = format(subYears(new Date(selectedDate.getFullYear(), selectedDate.getMonth(), 1), 1), 'yyyy-MM-dd')
        const currDate = format(subDays(selectedDate, 2), 'yyyy-MM-dd')
        const prevDate = format(subMonths(subDays(selectedDate, 2), 1), 'yyyy-MM-dd')
        const prevYearCurrDate = format(subYears(subDays(selectedDate, 2), 1), 'yyyy-MM-dd')

        const p1 = db
            .select({
                regionalName: regionals.regional,
                branchName: branches.branchNew,
                subbranchName: subbranches.subbranchNew,
                clusterName: clusters.cluster,
                kabupatenName: kabupatens.kabupaten,
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

        const [targetRevenue] = await Promise.all([
            p1.execute()
        ])

        return Response.json({ data: targetRevenue }, { status: 200 })
    }, {
        query: t.Object({
            date: t.Optional(t.Date()),
            branch: t.Optional(t.String()),
            subbranch: t.Optional(t.String()),
            cluster: t.Optional(t.String()),
            kabupaten: t.Optional(t.String())
        })
    })
    .onError(({ code }) => {
        if (code === 'NOT_FOUND') {
            return Response.json({ message: 'Route not found :(' }, { status: 404 })
        }
    })
    .listen(8000)

console.log(`Running on http://localhost:8000`);
