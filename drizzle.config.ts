import { defineConfig } from "drizzle-kit";

export default defineConfig({
    dialect: "mysql",
    schema: "./database/schema.ts",
    out: "./drizzle",
    strict: true,
    verbose: true,
});
