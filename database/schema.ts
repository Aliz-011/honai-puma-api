import { mysqlSchema, varchar, decimal } from "drizzle-orm/mysql-core";
import { relations } from "drizzle-orm";

export const pumaSchema = mysqlSchema("puma_2025");

export const regionals = pumaSchema.table("regionals", {
    id: varchar("id", { length: 100 }).primaryKey(),
    regional: varchar("regional", { length: 40 }).notNull(),
});

export const regionalRelations = relations(regionals, ({ many }) => ({
    branches: many(branches),
}));

export const branches = pumaSchema.table("branches_new", {
    id: varchar("id", { length: 100 }).primaryKey(),
    regionalId: varchar("id_regional", { length: 100 }).notNull(),
    branchNew: varchar("branch_new", { length: 30 }).notNull(),
});

export const branchRelations = relations(branches, ({ one, many }) => ({
    regional: one(regionals, {
        fields: [branches.regionalId],
        references: [regionals.id],
    }),
    subbranches: many(subbranches),
}));

export const subbranches = pumaSchema.table("subbranches_new", {
    id: varchar("id", { length: 100 }).primaryKey(),
    branchId: varchar("id_branch", { length: 100 }).notNull(),
    subbranchNew: varchar("subbranch_new", { length: 30 }).notNull(),
});

export const subbranchRelations = relations(subbranches, ({ one, many }) => ({
    branch: one(branches, {
        fields: [subbranches.branchId],
        references: [branches.id],
    }),
    clusters: many(clusters),
}));

export const clusters = pumaSchema.table("clusters_new", {
    id: varchar("id", { length: 100 }).primaryKey(),
    subbranchId: varchar("subbranch_id", { length: 100 }).notNull(),
    cluster: varchar("cluster", { length: 30 }).notNull(),
});

export const clusterRelations = relations(clusters, ({ one, many }) => ({
    subbranch: one(subbranches, {
        fields: [clusters.subbranchId],
        references: [subbranches.id],
    }),
    kabupatens: many(kabupatens),
}));

export const kabupatens = pumaSchema.table("kabupatens", {
    id: varchar({ length: 100 }).primaryKey(),
    clusterId: varchar("id_cluster", { length: 100 }).notNull(),
    kabupaten: varchar({ length: 30 }).notNull(),
});

export const kabupatenRelations = relations(kabupatens, ({ one, many }) => ({
    cluster: one(clusters, {
        fields: [kabupatens.clusterId],
        references: [clusters.id],
    }),
    revenueGrosses: many(revenueGrosses),
    revenueByu: many(revenueByu)
}));

export const revenueGrosses = pumaSchema.table("Target_revenue_gross_2025", {
    id: varchar("id", { length: 100 }).primaryKey(),
    kabupatenId: varchar("id_kabupaten", { length: 100 }).notNull(),
    m1: decimal("m1", { precision: 18, scale: 2 }),
    m2: decimal("m2", { precision: 18, scale: 2 }),
    m3: decimal("m3", { precision: 18, scale: 2 }),
    m4: decimal("m4", { precision: 18, scale: 2 }),
    m5: decimal("m5", { precision: 18, scale: 2 }),
    m6: decimal("m6", { precision: 18, scale: 2 }),
    m7: decimal("m7", { precision: 18, scale: 2 }),
    m8: decimal("m8", { precision: 18, scale: 2 }),
    m9: decimal("m9", { precision: 18, scale: 2 }),
    m10: decimal("m10", { precision: 18, scale: 2 }),
    m11: decimal("m11", { precision: 18, scale: 2 }),
    m12: decimal("m12", { precision: 18, scale: 2 }),
    year: varchar({ length: 5 }).notNull(),
});

export const revenueGrossRelations = relations(revenueGrosses, ({ one }) => ({
    kabupaten: one(kabupatens, {
        fields: [revenueGrosses.kabupatenId],
        references: [kabupatens.id],
    }),
}));

export const revenueByu = pumaSchema.table("Target_revenue_byu_2025", {
    id: varchar("id", { length: 100 }).primaryKey(),
    kabupatenId: varchar("id_kabupaten", { length: 100 }).notNull(),
    m1: decimal("m1", { precision: 18, scale: 7 }),
    m2: decimal("m2", { precision: 18, scale: 7 }),
    m3: decimal("m3", { precision: 18, scale: 7 }),
    m4: decimal("m4", { precision: 18, scale: 7 }),
    m5: decimal("m5", { precision: 18, scale: 7 }),
    m6: decimal("m6", { precision: 18, scale: 7 }),
    m7: decimal("m7", { precision: 18, scale: 7 }),
    m8: decimal("m8", { precision: 18, scale: 7 }),
    m9: decimal("m9", { precision: 18, scale: 7 }),
    m10: decimal("m10", { precision: 18, scale: 7 }),
    m11: decimal("m11", { precision: 18, scale: 7 }),
    m12: decimal("m12", { precision: 18, scale: 7 }),
    year: varchar({ length: 5 }).notNull(),
});

export const revenueByuRelations = relations(revenueByu, ({ one }) => ({
    kabupaten: one(kabupatens, {
        fields: [revenueByu.kabupatenId],
        references: [kabupatens.id],
    }),
}));

export type Kabupaten = typeof kabupatens.$inferSelect
export type Cluster = typeof clusters.$inferSelect
export type Subbranch = typeof subbranches.$inferSelect
export type Branch = typeof branches.$inferSelect
export type Regional = typeof regionals.$inferSelect

export const table = {
    kabupatens,
    clusters,
    subbranches,
    branches,
    regionals,
    revenueGrosses,
    revenueByu,
} as const

export type Table = typeof table