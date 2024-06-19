use std::collections::{BTreeMap, BTreeSet};

use models::auth::role::{CustomTenantRole, TenantRoleIdentifier};
use models::auth::user::{UserDesc, ROOT};
use models::oid::{Identifier, Oid};
use models::schema::database_schema::DatabaseSchema;
use models::schema::table_schema::TableSchema;
use models::schema::tenant::Tenant;
use models::schema::{DEFAULT_CATALOG, DEFAULT_DATABASE, USAGE_SCHEMA};
use models::sql::{add_member_to_sql, create_table_sqls, role_to_sql, ToDDLSql};

use crate::error::MetaResult;
use crate::store::key_path::KeyPath;
use crate::store::storage::StateMachine;

pub async fn dump_impl(
    cluster: &str,
    tenant_filter: Option<&str>,
    storage: &StateMachine,
) -> MetaResult<String> {
    let tenants = KeyPath::tenants(cluster);
    let tenants = storage
        .children_data::<Tenant>(&tenants)?
        .into_iter()
        .collect::<BTreeMap<_, _>>();

    let users = KeyPath::users(cluster);
    let users = storage
        .children_data::<UserDesc>(&users)?
        .into_iter()
        .collect::<BTreeMap<_, _>>();

    let users = users
        .into_values()
        .map(|u| (*u.id(), u))
        .collect::<BTreeMap<_, _>>();

    let mut users_used: BTreeSet<String> = BTreeSet::new();
    let mut tenant_object_dump = vec![];
    for (_, tenant) in tenants.iter() {
        if tenant_filter.is_some_and(|f| tenant.name().eq(f)) || tenant_filter.is_none() {
            tenant_object_dump.push(format!("-- Dump Tenant {} Object", tenant.name()));
            tenant_object_dump.append(&mut dump_tenant(
                storage,
                cluster,
                tenant.name(),
                &users,
                &mut users_used,
            )?);
        }
    }

    let mut res = vec![];
    res.push("-- Dump Global Object".to_string());
    for (_, tenant) in tenants.iter() {
        if tenant.name().eq_ignore_ascii_case(DEFAULT_CATALOG) {
            continue;
        }
        if tenant_filter.is_some_and(|f| tenant.name().eq(f)) || tenant_filter.is_none() {
            res.push(tenant.to_ddl_sql(true)?)
        }
    }

    let mut users = users.into_values().collect::<Vec<_>>();
    users.sort_by_key(|u| u.name().to_string());

    for u in users {
        if u.name().eq_ignore_ascii_case(ROOT) {
            continue;
        }

        if tenant_filter.is_none() {
            res.push(u.to_ddl_sql(false)?);
        } else if tenant_filter.is_some() && users_used.contains(u.name()) {
            res.push(u.to_ddl_sql(true)?);
        }
    }

    res.append(&mut tenant_object_dump);

    let mut res = res.join("\n");
    res.push('\n');
    Ok(res)
}

pub fn dump_tenant(
    storage: &StateMachine,
    cluster: &str,
    tenant: &str,
    user_table: &BTreeMap<Oid, UserDesc>,
    user_used: &mut BTreeSet<String>,
) -> MetaResult<Vec<String>> {
    let mut res = vec![];
    res.push(format!(r#"\change_tenant {}"#, tenant));
    let dbs_key = KeyPath::tenant_dbs(cluster, tenant);
    let dbs = storage
        .children_data::<DatabaseSchema>(&dbs_key)?
        .into_iter()
        .collect::<BTreeMap<_, _>>();

    // dump database
    for (_name, db) in dbs.iter() {
        if db.tenant_name().eq_ignore_ascii_case(DEFAULT_CATALOG)
            && db.database_name().eq_ignore_ascii_case(DEFAULT_DATABASE)
        {
            continue;
        }
        res.push(db.to_ddl_sql(true)?)
    }

    // dump role
    let roles_key = KeyPath::roles(cluster, tenant);
    let roles = storage
        .children_data::<CustomTenantRole<Oid>>(&roles_key)?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    for (_, role) in roles.iter() {
        res.append(&mut role_to_sql(role)?)
    }

    // dump member
    let members_key = KeyPath::members(cluster, tenant);
    let mut members = storage
        .children_data::<TenantRoleIdentifier>(&members_key)?
        .into_iter()
        .filter_map(|(o, r)| {
            o.parse::<Oid>()
                .ok()
                .and_then(|o| user_table.get(&o))
                .map(|u| {
                    user_used.insert(u.name().into());
                    (u, r)
                })
        })
        .collect::<Vec<_>>();
    members.sort_by_key(|(u, _)| u.name());

    for (user, role) in members.into_iter() {
        if user.name().eq_ignore_ascii_case(ROOT) && tenant.eq_ignore_ascii_case(DEFAULT_CATALOG) {
            continue;
        }
        res.push(add_member_to_sql(tenant, user.name(), role.name()))
    }

    // dump table
    for (_, db) in dbs {
        let tables = KeyPath::tenant_schemas(cluster, tenant, db.database_name());
        let mut schemas = storage
            .children_data::<TableSchema>(&tables)?
            .into_values()
            .collect::<Vec<_>>();
        schemas.sort_by_key(|s| format!("{}.{}", s.db(), s.name()));
        if db.database_name().eq_ignore_ascii_case(USAGE_SCHEMA) {
            res.append(&mut create_table_sqls(&schemas, true)?);
        } else {
            res.append(&mut create_table_sqls(&schemas, false)?)
        }
    }

    Ok(res)
}
