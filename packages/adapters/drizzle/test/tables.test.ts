import { describe, expect, it } from 'vitest';
import { tablesFromSql } from '../src/index';

describe('drizzle adapter — table derivation (F-022)', () => {
  const sql =
    'select "sales"."id", "sales"."sold_at", "customers"."name", "users"."display_name" from "sales" ' +
    'inner join "customers" on "customers"."id" = "sales"."customer_id" ' +
    'inner join "users" on "users"."id" = "sales"."seller_id" where "sales"."org_id" = ? order by "sales"."sold_at" desc limit ?';

  it('derives FROM and JOIN tables without a schema', () => {
    expect(tablesFromSql(sql).sort()).toEqual(['customers', 'sales', 'users']);
  });

  it('intersects with the known schema and picks up subquery references', () => {
    const known = new Set(['sales', 'customers', 'users', 'sale_items']);
    const withSub = sql.replace('where', 'where (select count(*) from "sale_items" where "sale_items"."sale_id" = "sales"."id") > 0 and');
    expect(tablesFromSql(withSub, known).sort()).toEqual(['customers', 'sale_items', 'sales', 'users']);
    expect(tablesFromSql(sql, new Set(['sales']))).toEqual(['sales']);
  });
});
