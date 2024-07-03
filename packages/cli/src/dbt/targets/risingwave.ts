import {
    CreateRisingWaveCredentials,
    ParseError,
    WarehouseTypes,
} from '@lightdash/common';
import { JSONSchemaType } from 'ajv';
import betterAjvErrors from 'better-ajv-errors';
import { ajv } from '../../ajv';
import { Target } from '../types';

export type RisingWaveTarget = {
    type: 'risingwave';
    host: string;
    user: string;
    port: number;
    dbname?: string;
    database?: string;
    schema: string;
    threads: number;
    pass?: string;
    password?: string;
    keepalives_idle?: number;
    connect_timeout?: number;
    search_path?: string;
    role?: string;
    sslmode?: string;
};

export const risingWaveSchema: JSONSchemaType<RisingWaveTarget> = {
    type: 'object',
    properties: {
        type: {
            type: 'string',
            enum: ['risingwave'],
        },
        host: {
            type: 'string',
        },
        user: {
            type: 'string',
        },
        port: {
            type: 'integer',
        },
        dbname: {
            type: 'string',
            nullable: true,
        },
        database: {
            type: 'string',
            nullable: true,
        },
        schema: {
            type: 'string',
        },
        threads: {
            type: 'integer',
            nullable: true,
        },
        pass: {
            type: 'string',
            nullable: true,
        },
        password: {
            type: 'string',
            nullable: true,
        },
        keepalives_idle: {
            type: 'integer',
            nullable: true,
        },
        connect_timeout: {
            type: 'integer',
            nullable: true,
        },
        search_path: {
            type: 'string',
            nullable: true,
        },
        role: {
            type: 'string',
            nullable: true,
        },
        sslmode: {
            type: 'string',
            nullable: true,
        },
    },
    required: ['type', 'host', 'user', 'port', 'schema'],
};

export const convertRisingWaveSchema = (
    target: Target,
): CreateRisingWaveCredentials => {
    const validate = ajv.compile<RisingWaveTarget>(risingWaveSchema);
    if (validate(target)) {
        const password = target.pass || target.password;
        if (!password) {
            throw new ParseError(
                `RisingWave target requires a password: "password"`,
            );
        }
        const dbname = target.dbname || target.database;
        if (!dbname) {
            throw new ParseError(
                `RisingWave target requires a database name: "database"`,
            );
        }
        return {
            type: WarehouseTypes.RISINGWAVE,
            host: target.host,
            user: target.user,
            password,
            port: target.port,
            dbname,
            schema: target.schema,
            keepalivesIdle: target.keepalives_idle,
            searchPath: target.search_path,
            role: target.role,
            sslmode: target.sslmode,
        };
    }
    const errs = betterAjvErrors(risingWaveSchema, target, validate.errors || []);
    throw new ParseError(
        `Couldn't read a profiles.yml file for ${target.type}:\n${errs}`,
    );
};
