// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data.SqlClient;
using System.Threading;
using Microsoft.EntityFrameworkCore.Storage;

namespace Microsoft.EntityFrameworkCore.SqlServer.FunctionalTests.Utilities
{
    public class TestSqlAzureExecutionStrategy : SqlAzureExecutionStrategy
    {
        private static readonly int[] _additionalErrorNumbers =
        {
            -1, // Physical connection is not usable
            -2, // Timeout
            42008, // Mirroring (pending delete)
            42019, // CREATE DATABASE operation failed
            49918 // Not enough resources to process request
        };

        public TestSqlAzureExecutionStrategy()
            : base(DefaultMaxRetryCount, DefaultMaxDelay, _additionalErrorNumbers)
        {
        }

        public TestSqlAzureExecutionStrategy(DbContext context)
            : base(context, DefaultMaxRetryCount, DefaultMaxDelay, _additionalErrorNumbers)
        {
        }

        public TestSqlAzureExecutionStrategy(ExecutionStrategyContext context)
            : base(context, DefaultMaxRetryCount, DefaultMaxDelay, _additionalErrorNumbers)
        {
        }

        protected override bool ShouldRetryOn(Exception exception)
        {
            if (base.ShouldRetryOn(exception))
            {
                return true;
            }

            var sqlException = exception as SqlException;
            if (sqlException != null)
            {
                var message = "Didn't retry on";
                foreach (SqlError err in sqlException.Errors)
                {
                    message += " " + err.Number;
                }
                throw new InvalidOperationException(message, exception);
            }

            var invalidOperationException = exception as InvalidOperationException;
            if (invalidOperationException != null
                && invalidOperationException.Message == "Internal .Net Framework Data Provider error 6.")
            {
                return true;
            }

            return false;
        }

        public new static bool Suspended
        {
            get { return ExecutionStrategy.Suspended; }
            set { ExecutionStrategy.Suspended = value; }
        }
    }
}
