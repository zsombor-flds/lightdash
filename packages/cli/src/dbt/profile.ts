import { CreateWarehouseCredentials, ParseError } from '@lightdash/common';
import { promises as fs } from 'fs';
import * as yaml from 'js-yaml';
import * as path from 'path';
import { convertBigquerySchema } from './targets/Bigquery';
import { convertDatabricksSchema } from './targets/databricks';
import { convertPostgresSchema } from './targets/postgres';
import { convertRisingWaveSchema } from './targets/risingwave';
import { convertRedshiftSchema } from './targets/redshift';
import { convertSnowflakeSchema } from './targets/snowflake';
import { convertTrinoSchema } from './targets/trino';
import { renderProfilesYml } from './templating';
import { LoadProfileArgs, Profiles, Target } from './types';

export const loadDbtTarget = async ({
    profilesDir,
    profileName,
    targetName,
}: LoadProfileArgs): Promise<{ name: string; target: Target }> => {
    const profilePath = path.join(profilesDir, 'profiles.yml');
    let allProfiles;
    try {
        const raw = await fs.readFile(profilePath, { encoding: 'utf8' });
        const rendered = renderProfilesYml(raw);
        allProfiles = yaml.load(rendered) as Profiles;
    } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : '-';
        throw new ParseError(
            `Could not find a valid profiles.yml file at ${profilePath}:\n  ${msg}`,
        );
    }
    const profile = allProfiles[profileName];
    if (!profile) {
        throw new ParseError(
            `Profile ${profileName} not found in ${profilePath}`,
        );
    }
    const selectedTargetName = targetName || profile.target;
    const target = profile.outputs[selectedTargetName];
    if (target === undefined) {
        throw new ParseError(
            `Couldn't find target "${selectedTargetName}" for profile ${profileName} in profiles.yml at ${profilePath}`,
        );
    }
    return {
        name: selectedTargetName,
        target,
    };
};

export const warehouseCredentialsFromDbtTarget = async (
    target: Target,
): Promise<CreateWarehouseCredentials> => {
    switch (target.type) {
        case 'risingwave':
            return convertRisingWaveSchema(target);
        case 'postgres':
            return convertPostgresSchema(target);
        case 'snowflake':
            return convertSnowflakeSchema(target);
        case 'bigquery':
            return convertBigquerySchema(target);
        case 'redshift':
            return convertRedshiftSchema(target);
        case 'databricks':
            return convertDatabricksSchema(target);
        case 'trino':
            return convertTrinoSchema(target);
        default:
            throw new ParseError(
                `Sorry! Lightdash doesn't yet support ${target.type} dbt targets`,
            );
    }
};
