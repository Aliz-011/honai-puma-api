import { table } from './schema'
import { spreads } from './utils'

export const model = {
    select: spreads({
        kabupatens: table.kabupatens,
        clusters: table.clusters,
        subbranches: table.subbranches,
        branches: table.branches,
        regionals: table.regionals,
        revenueGrosses: table.revenueGrosses,
        revenueByu: table.revenueByu
    }, 'select')
} as const