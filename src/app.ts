import { Elysia, t } from "elysia";
import { swagger } from '@elysiajs/swagger'
import { and, asc, between, eq, isNotNull, sql } from "drizzle-orm";
import { subMonths, subDays, format, subYears } from 'date-fns'
import type { AnyMySqlTable } from "drizzle-orm/mysql-core";

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

        const { branchSums, clusSums, kabSums, regSums, subSums } = getRegClassPrabayar(currRev, db2, firstDayOfCurrMonth, currDate)
        const { branchSums: branchSumsP3, clusSums: clusSumsP3, kabSums: kabSumsP3, regSums: regSumsP3, subSums: subSumsP3 } = getRegClassPrabayar(prevMonthRev, db2, firstDayOfPrevMonth, prevDate)
        const { branchSums: branchSumsP4, clusSums: clusSumsP4, kabSums: kabSumsP4, regSums: regSumsP4, subSums: subSumsP4 } = getRegClassPrabayar(prevYearCurrMonthRev, db2, firstDayOfPrevYearCurrMonth, prevYearCurrDate)

        // QUERY UNTUK TARGET BULAN INI
        const p1 = db
            .select({
                regionalName: regionals.regional,
                branchName: branches.branchNew,
                subbranchName: subbranches.subbranchNew,
                clusterName: clusters.cluster,
                kabupatenName: kabupatens.kabupaten,
                totalRevenue: sql<number>`CAST(SUM(${revenueGrosses[monthColumn]}) AS DOUBLE PRECISION)`.as('totalRevenue')
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

        // QUERY UNTUK PENDAPATAN BULAN INI
        const p2 = db2
            .select({
                region: sql<string>`${kabSums.region}`.as('region'),
                branch: sql<string>`${kabSums.branch}`.as('branch'), // Keep only one branchName
                subbranch: kabSums.subbranch,
                cluster: kabSums.cluster,
                kabupaten: sql<string>`${kabSums.kabupaten}`.as('kabupaten'),
                kabupatenRev: kabSums.kabupatenRev,
                clusterRev: clusSums.clusterRev,
                subbranchRev: subSums.subbranchRev,
                branchRev: branchSums.branchRev,
                regionalRev: regSums.regionalRev
            })
            .from(kabSums)
            .innerJoin(clusSums, and(
                and(eq(kabSums.region, clusSums.region), eq(kabSums.branch, clusSums.branch)),
                and(eq(kabSums.subbranch, clusSums.subbranch), eq(kabSums.cluster, clusSums.cluster))
            ))
            .innerJoin(subSums, and(
                eq(kabSums.region, subSums.region),
                and(eq(kabSums.branch, subSums.branch), eq(kabSums.subbranch, subSums.subbranch))
            ))
            .innerJoin(branchSums, and(eq(kabSums.region, branchSums.region), eq(kabSums.branch, branchSums.branch)))
            .innerJoin(regSums, eq(kabSums.region, regSums.regionName))
            .orderBy(kabSums.region, kabSums.branch, kabSums.subbranch, kabSums.cluster, kabSums.kabupaten)
            .prepare()

        // QUERY UNTUK PENDAPATAN BULAN SEBELUMNYA
        const p3 = db2
            .select({
                region: sql<string>`${kabSumsP3.region}`.as('region'),
                branch: sql<string>`${kabSumsP3.branch}`.as('branch'), // Keep only one branchName
                subbranch: kabSumsP3.subbranch,
                cluster: kabSumsP3.cluster,
                kabupaten: sql<string>`${kabSumsP3.kabupaten}`.as('kabupaten'),
                kabupatenRev: kabSumsP3.kabupatenRev,
                clusterRev: clusSumsP3.clusterRev,
                subbranchRev: subSumsP3.subbranchRev,
                branchRev: branchSumsP3.branchRev,
                regionalRev: regSumsP3.regionalRev
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
                kabupatenRev: kabSumsP4.kabupatenRev,
                clusterRev: clusSumsP4.clusterRev,
                subbranchRev: subSumsP4.subbranchRev,
                branchRev: branchSumsP4.branchRev,
                regionalRev: regSumsP4.regionalRev
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

        return Response.json({ targetRevenue, currMonthRevenue, prevMonthRevenue, prevYearCurrMonthRevenue }, { status: 200 })
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

        const { branchSums, clusSums, kabSums, regSums, subSums } = getRegClassByu(currRevByu, db3, firstDayOfCurrMonth, currDate)
        const { branchSums: branchSumsP3, clusSums: clusSumsP3, kabSums: kabSumsP3, regSums: regSumsP3, subSums: subSumsP3 } = getRegClassByu(prevMonthRevByu, db3, firstDayOfPrevMonth, prevDate)
        const { branchSums: branchSumsP4, clusSums: clusSumsP4, kabSums: kabSumsP4, regSums: regSumsP4, subSums: subSumsP4 } = getRegClassByu(prevYearCurrMonthRevByu, db3, firstDayOfPrevYearCurrMonth, prevYearCurrDate)

        // QUERY UNTUK TARGET BULAN INI
        const p1 = db
            .select({
                id: regionals.id,
                regionalName: regionals.regional,
                branchName: branches.branchNew,
                subbranchName: subbranches.subbranchNew,
                clusterName: clusters.cluster,
                kabupatenName: kabupatens.kabupaten,
                totalRevenue: sql<number>`CAST(SUM(${revenueByu[monthColumn]}) AS DOUBLE PRECISION)`.as('totalRevenue')
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
                region: sql<string>`${kabSums.region}`.as('region'),
                branch: sql<string>`${kabSums.branch}`.as('branch'), // Keep only one branchName
                subbranch: sql<string>`${kabSums.subbranch}`.as('subbranch'),
                cluster: sql<string>`${kabSums.cluster}`.as('cluster'),
                kabupaten: sql<string>`${kabSums.kabupaten}`.as('kabupaten'),
                kabupatenRev: kabSums.kabupatenRev,
                clusterRev: clusSums.clusterRev,
                subbranchRev: subSums.subbranchRev,
                branchRev: branchSums.branchRev,
                regionalRev: regSums.regionalRev
            })
            .from(kabSums)
            .innerJoin(clusSums, and(
                and(eq(kabSums.region, clusSums.region), eq(kabSums.branch, clusSums.branch)),
                and(eq(kabSums.subbranch, clusSums.subbranch), eq(kabSums.cluster, clusSums.cluster))
            ))
            .innerJoin(subSums, and(
                eq(kabSums.region, subSums.region),
                and(eq(kabSums.branch, subSums.branch), eq(kabSums.subbranch, subSums.subbranch))
            ))
            .innerJoin(branchSums, and(eq(kabSums.region, branchSums.region), eq(kabSums.branch, branchSums.branch)))
            .innerJoin(regSums, eq(kabSums.region, regSums.regionName))
            .orderBy(kabSums.region, kabSums.branch, kabSums.subbranch, kabSums.cluster, kabSums.kabupaten)
            .prepare()

        // QUERY UNTUK MENDAPAT PREV MONTH REVENUE
        const p3 = db3
            .select({
                region: sql<string>`${kabSumsP3.region}`.as('region'),
                branch: sql<string>`${kabSumsP3.branch}`.as('branch'), // Keep only one branchName
                subbranch: kabSumsP3.subbranch,
                cluster: kabSumsP3.cluster,
                kabupaten: sql<string>`${kabSumsP3.kabupaten}`.as('kabupaten'),
                kabupatenRev: kabSumsP3.kabupatenRev,
                clusterRev: clusSumsP3.clusterRev,
                subbranchRev: subSumsP3.subbranchRev,
                branchRev: branchSumsP3.branchRev,
                regionalRev: regSumsP3.regionalRev
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
                kabupatenRev: kabSumsP4.kabupatenRev,
                clusterRev: clusSumsP4.clusterRev,
                subbranchRev: subSumsP4.subbranchRev,
                branchRev: branchSumsP4.branchRev,
                regionalRev: regSumsP4.regionalRev
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

        return Response.json({ targetRevenue, currMonthRevenue, prevMonthRevenue, prevYearRevenue }, { status: 200 })
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
        return Response.json({ data: 'hello' }, { status: 200 })
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

console.log(`Listening on http://localhost:8000`);

function getRegClassByu<T extends AnyMySqlTable>(table: any, db: any, firstDayOfCurrMonth: string, currDay: string) {
    const regClass = db.select({
        msisdn: table.msisdn,
        periodde: table.periode,
        eventDate: table.eventDate,
        rev: table.rev,
        regionName: table.regionSales,
        kabupatenName: table.kabupaten,
        branchName: sql<string>`
CASE
    WHEN ${table.kabupaten} IN (
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
    WHEN ${table.kabupaten} IN (
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
    WHEN ${table.kabupaten} IN (
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
    WHEN ${table.kabupaten} IN (
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
    WHEN ${table.kabupaten} IN (
        'AMBON',
        'KOTA AMBON',
        'MALUKU TENGAH',
        'SERAM BAGIAN TIMUR'
    ) THEN 'AMBON'
    WHEN ${table.kabupaten} IN (
        'KEPULAUAN ARU',
        'KOTA TUAL',
        'MALUKU BARAT DAYA',
        'MALUKU TENGGARA',
        'MALUKU TENGGARA BARAT',
        'KEPULAUAN TANIMBAR'
    ) THEN 'KEPULAUAN AMBON'
    WHEN ${table.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
    WHEN ${table.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
    WHEN ${table.kabupaten} IN (
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
    WHEN ${table.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
    WHEN ${table.kabupaten} IN (
        'FAKFAK',
        'FAK FAK',
        'KAIMANA',
        'MANOKWARI SELATAN',
        'PEGUNUNGAN ARFAK',
        'TELUK BINTUNI',
        'TELUK WONDAMA'
    ) THEN 'MANOKWARI OUTER'
    WHEN ${table.kabupaten} IN (
        'KOTA SORONG',
        'MAYBRAT',
        'RAJA AMPAT',
        'SORONG',
        'SORONG SELATAN',
        'TAMBRAUW'
    ) THEN 'SORONG RAJA AMPAT'
    WHEN ${table.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
    WHEN ${table.kabupaten} IN (
        'INTAN JAYA',
        'MIMIKA',
        'PUNCAK',
        'PUNCAK JAYA',
        'TIMIKA'
    ) THEN 'MIMIKA'
    WHEN ${table.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
    ELSE NULL
END
    `.as('subbranchName'),
        clusterName: sql<string>`
CASE
    WHEN ${table.kabupaten} IN (
        'KOTA AMBON',
        'MALUKU TENGAH',
        'SERAM BAGIAN TIMUR'
    ) THEN 'AMBON'
    WHEN ${table.kabupaten} IN (
        'KEPULAUAN ARU',
        'KOTA TUAL',
        'MALUKU BARAT DAYA',
        'MALUKU TENGGARA',
        'MALUKU TENGGARA BARAT',
        'KEPULAUAN TANIMBAR'
    ) THEN 'KEPULAUAN TUAL'
    WHEN ${table.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
    WHEN ${table.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
    WHEN ${table.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
    WHEN ${table.kabupaten} IN (
        'BIAK',
        'BIAK NUMFOR',
        'KEPULAUAN YAPEN',
        'SUPIORI',
        'WAROPEN'
    ) THEN 'NEW BIAK NUMFOR'
    WHEN ${table.kabupaten} IN (
        'JAYAWIJAYA',
        'LANNY JAYA',
        'MAMBERAMO TENGAH',
        'NDUGA',
        'PEGUNUNGAN BINTANG',
        'TOLIKARA',
        'YAHUKIMO',
        'YALIMO'
    ) THEN 'PAPUA PEGUNUNGAN'
    WHEN ${table.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
    WHEN ${table.kabupaten} IN (
        'FAKFAK',
        'FAK FAK',
        'KAIMANA',
        'MANOKWARI SELATAN',
        'PEGUNUNGAN ARFAK',
        'TELUK BINTUNI',
        'TELUK WONDAMA'
    ) THEN 'MANOKWARI OUTER'
    WHEN ${table.kabupaten} IN (
        'KOTA SORONG',
        'MAYBRAT',
        'RAJA AMPAT',
        'SORONG',
        'SORONG SELATAN',
        'TAMBRAUW'
    ) THEN 'NEW SORONG RAJA AMPAT'
    WHEN ${table.kabupaten} IN (
        'INTAN JAYA',
        'MIMIKA',
        'PUNCAK',
        'PUNCAK JAYA',
        'TIMIKA'
    ) THEN 'MIMIKA PUNCAK'
    WHEN ${table.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
    WHEN ${table.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
    ELSE NULL
END
    `.as('clusterName'),
    })
        .from(table)
        .where(between(table.eventDate, firstDayOfCurrMonth, currDay))
        .as('regionClassififcation')

    const kabSums = db
        .select({
            region: regClass.regionName,
            branch: sql<string>`${regClass.branchName}`.as('kabBranch'),
            subbranch: sql<string>`${regClass.subbranchName}`.as('kabSubbranch'),
            cluster: sql<string>`${regClass.clusterName}`.as('kabCluster'),
            kabupaten: regClass.kabupatenName,
            kabupatenRev: sql<number>`SUM(${regClass.rev})`.as('kabupatenRev')
        })
        .from(regClass)
        .where(isNotNull(regClass.branchName))
        .groupBy(regClass.regionName, regClass.branchName, regClass.subbranchName, regClass.clusterName, regClass.kabupatenName)
        .as('kabSums')

    const clusSums = db
        .select({
            region: kabSums.region,
            branch: sql<string>`${kabSums.branch}`.as('clusBranch'),
            subbranch: sql<string>`${kabSums.subbranch}`.as('clusSubbranch'),
            cluster: sql<string>`${kabSums.cluster}`.as('cluster'),
            clusterRev: sql<number>`SUM(${kabSums.kabupatenRev})`.as('clusterRev')
        })
        .from(kabSums)
        .groupBy(kabSums.region, kabSums.branch, kabSums.subbranch, kabSums.cluster)
        .as('clusSums')

    const subSums = db
        .select({
            region: clusSums.region,
            branch: sql<string>`${clusSums.branch}`.as('subSumsBranch'),
            subbranch: sql<string>`${clusSums.subbranch}`.as('subbranch'),
            subbranchRev: sql<number>`SUM(${clusSums.clusterRev})`.as('subbranchRev')
        })
        .from(clusSums)
        .groupBy(clusSums.region, clusSums.branch, clusSums.subbranch)
        .as('subSums')

    const branchSums = db
        .select({
            region: subSums.region,
            branch: sql`${subSums.branch}`.as('branch'),
            branchRev: sql<number>`SUM(${subSums.subbranchRev})`.as('branchRev')
        })
        .from(subSums)
        .groupBy(subSums.region, subSums.branch)
        .as('branchSums')

    const regSums = db
        .select({
            regionName: branchSums.region,
            regionalRev: sql<number>`SUM(${branchSums.branchRev})`.as('regionalRev')
        })
        .from(branchSums)
        .groupBy(branchSums.region)
        .as('regSums')

    return { regClass, branchSums, kabSums, clusSums, subSums, regSums }
}

function getRegClassPrabayar<T extends AnyMySqlTable>(table: any, db: any, firstDayOfCurrMonth: string, currDay: string) {
    const regClass = db.select({
        mtdDt: table.mtdDt,
        rev: table.rev,
        regionName: table.regionSales,
        kabupatenName: table.kabupaten,
        branchName: sql<string>`
CASE
    WHEN ${table.kabupaten} IN (
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
    WHEN ${table.kabupaten} IN (
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
    WHEN ${table.kabupaten} IN (
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
    WHEN ${table.kabupaten} IN (
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
    WHEN ${table.kabupaten} IN (
        'AMBON',
        'KOTA AMBON',
        'MALUKU TENGAH',
        'SERAM BAGIAN TIMUR'
    ) THEN 'AMBON'
    WHEN ${table.kabupaten} IN (
        'KEPULAUAN ARU',
        'KOTA TUAL',
        'MALUKU BARAT DAYA',
        'MALUKU TENGGARA',
        'MALUKU TENGGARA BARAT',
        'KEPULAUAN TANIMBAR'
    ) THEN 'KEPULAUAN AMBON'
    WHEN ${table.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
    WHEN ${table.kabupaten} IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
    WHEN ${table.kabupaten} IN (
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
    WHEN ${table.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
    WHEN ${table.kabupaten} IN (
        'FAKFAK',
        'FAK FAK',
        'KAIMANA',
        'MANOKWARI SELATAN',
        'PEGUNUNGAN ARFAK',
        'TELUK BINTUNI',
        'TELUK WONDAMA'
    ) THEN 'MANOKWARI OUTER'
    WHEN ${table.kabupaten} IN (
        'KOTA SORONG',
        'MAYBRAT',
        'RAJA AMPAT',
        'SORONG',
        'SORONG SELATAN',
        'TAMBRAUW'
    ) THEN 'SORONG RAJA AMPAT'
    WHEN ${table.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
    WHEN ${table.kabupaten} IN (
        'INTAN JAYA',
        'MIMIKA',
        'PUNCAK',
        'PUNCAK JAYA',
        'TIMIKA'
    ) THEN 'MIMIKA'
    WHEN ${table.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
    ELSE NULL
END
    `.as('subbranchName'),
        clusterName: sql<string>`
CASE
    WHEN ${table.kabupaten} IN (
        'KOTA AMBON',
        'MALUKU TENGAH',
        'SERAM BAGIAN TIMUR'
    ) THEN 'AMBON'
    WHEN ${table.kabupaten} IN (
        'KEPULAUAN ARU',
        'KOTA TUAL',
        'MALUKU BARAT DAYA',
        'MALUKU TENGGARA',
        'MALUKU TENGGARA BARAT',
        'KEPULAUAN TANIMBAR'
    ) THEN 'KEPULAUAN TUAL'
    WHEN ${table.kabupaten} IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BARAT BURU'
    WHEN ${table.kabupaten} IN ('KOTA JAYAPURA') THEN 'KOTA JAYAPURA'
    WHEN ${table.kabupaten} IN ('JAYAPURA', 'KEEROM', 'MAMBERAMO RAYA', 'SARMI') THEN 'JAYAPURA OUTER'
    WHEN ${table.kabupaten} IN (
        'BIAK',
        'BIAK NUMFOR',
        'KEPULAUAN YAPEN',
        'SUPIORI',
        'WAROPEN'
    ) THEN 'NEW BIAK NUMFOR'
    WHEN ${table.kabupaten} IN (
        'JAYAWIJAYA',
        'LANNY JAYA',
        'MAMBERAMO TENGAH',
        'NDUGA',
        'PEGUNUNGAN BINTANG',
        'TOLIKARA',
        'YAHUKIMO',
        'YALIMO'
    ) THEN 'PAPUA PEGUNUNGAN'
    WHEN ${table.kabupaten} IN ('MANOKWARI') THEN 'MANOKWARI'
    WHEN ${table.kabupaten} IN (
        'FAKFAK',
        'FAK FAK',
        'KAIMANA',
        'MANOKWARI SELATAN',
        'PEGUNUNGAN ARFAK',
        'TELUK BINTUNI',
        'TELUK WONDAMA'
    ) THEN 'MANOKWARI OUTER'
    WHEN ${table.kabupaten} IN (
        'KOTA SORONG',
        'MAYBRAT',
        'RAJA AMPAT',
        'SORONG',
        'SORONG SELATAN',
        'TAMBRAUW'
    ) THEN 'NEW SORONG RAJA AMPAT'
    WHEN ${table.kabupaten} IN (
        'INTAN JAYA',
        'MIMIKA',
        'PUNCAK',
        'PUNCAK JAYA',
        'TIMIKA'
    ) THEN 'MIMIKA PUNCAK'
    WHEN ${table.kabupaten} IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
    WHEN ${table.kabupaten} IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'NEW MERAUKE'
    ELSE NULL
END
    `.as('clusterName'),
    })
        .from(table)
        .where(and(
            and(isNotNull(table.branch), isNotNull(table.subbranch)),
            between(table.mtdDt, firstDayOfCurrMonth, currDay)
        ))
        .as('regionClassififcation')

    const kabSums = db
        .select({
            region: regClass.regionName,
            branch: sql<string>`${regClass.branchName}`.as('kabBranch'),
            subbranch: sql<string>`${regClass.subbranchName}`.as('kabSubbranch'),
            cluster: sql<string>`${regClass.clusterName}`.as('kabCluster'),
            kabupaten: regClass.kabupatenName,
            kabupatenRev: sql<number>`SUM(${regClass.rev})`.as('kabupatenRev')
        })
        .from(regClass)
        .where(isNotNull(regClass.branchName))
        .groupBy(regClass.regionName, regClass.branchName, regClass.subbranchName, regClass.clusterName, regClass.kabupatenName)
        .as('kabSums')

    const clusSums = db
        .select({
            region: kabSums.region,
            branch: sql<string>`${kabSums.branch}`.as('clusBranch'),
            subbranch: sql<string>`${kabSums.subbranch}`.as('clusSubbranch'),
            cluster: sql<string>`${kabSums.cluster}`.as('cluster'),
            clusterRev: sql<number>`SUM(${kabSums.kabupatenRev})`.as('clusterRev')
        })
        .from(kabSums)
        .groupBy(kabSums.region, kabSums.branch, kabSums.subbranch, kabSums.cluster)
        .as('clusSums')

    const subSums = db
        .select({
            region: clusSums.region,
            branch: sql<string>`${clusSums.branch}`.as('subSumsBranch'),
            subbranch: sql<string>`${clusSums.subbranch}`.as('subbranch'),
            subbranchRev: sql<number>`SUM(${clusSums.clusterRev})`.as('subbranchRev')
        })
        .from(clusSums)
        .groupBy(clusSums.region, clusSums.branch, clusSums.subbranch)
        .as('subSums')

    const branchSums = db
        .select({
            region: subSums.region,
            branch: sql`${subSums.branch}`.as('branch'),
            branchRev: sql<number>`SUM(${subSums.subbranchRev})`.as('branchRev')
        })
        .from(subSums)
        .groupBy(subSums.region, subSums.branch)
        .as('branchSums')

    const regSums = db
        .select({
            regionName: branchSums.region,
            regionalRev: sql<number>`SUM(${branchSums.branchRev})`.as('regionalRev')
        })
        .from(branchSums)
        .groupBy(branchSums.region)
        .as('regSums')

    return { regClass, branchSums, kabSums, clusSums, subSums, regSums }
}

type Kabupaten = {
    kabupaten: string;
    targetRevenue: number;
}
type Cluster = {
    subbranch: string;
    targetRevenue: number;
    kabupatens: Kabupaten[]
}
type Subbranch = {
    subbranch: string;
    targetRevenue: number;
    clusters: Cluster[]
}

type Branch = {
    branch: string;
    targetRevenue: number;
    subbranches: Subbranch[]
}