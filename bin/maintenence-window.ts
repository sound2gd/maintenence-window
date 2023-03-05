#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { MaintenenceWindowStack } from '../lib/maintenence-window-stack';

const app = new cdk.App();
new MaintenenceWindowStack(app, 'MaintenenceWindowStack');
