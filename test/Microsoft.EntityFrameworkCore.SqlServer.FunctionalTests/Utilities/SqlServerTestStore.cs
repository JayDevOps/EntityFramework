// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Specification.Tests;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Internal;
using Microsoft.Extensions.DependencyInjection;

namespace Microsoft.EntityFrameworkCore.SqlServer.FunctionalTests.Utilities
{
    public class SqlServerTestStore : RelationalTestStore
    {
        public const int CommandTimeout = 90;

#if NETCOREAPP1_0
        private static string BaseDirectory => AppContext.BaseDirectory;
#else
        private static string BaseDirectory => AppDomain.CurrentDomain.BaseDirectory;
#endif

        public static SqlServerTestStore GetOrCreateShared(string name, Action initializeDatabase)
            => new SqlServerTestStore(name).CreateShared(initializeDatabase);

        /// <summary>
        ///     A non-transactional, transient, isolated test database. Use this in the case
        ///     where transactions are not appropriate.
        /// </summary>
        public static Task<SqlServerTestStore> CreateScratchAsync(bool createDatabase = true, bool useFileName = false)
            => new SqlServerTestStore(GetScratchDbName(), useFileName).CreateTransientAsync(createDatabase);

        public static SqlServerTestStore CreateScratch(bool createDatabase = true, bool useFileName = false)
            => new SqlServerTestStore(GetScratchDbName(), useFileName).CreateTransient(createDatabase);

        private SqlConnection _connection;
        private readonly string _fileName;
        private string _connectionString;
        private bool _deleteDatabase;

        public string Name { get; }
        public override string ConnectionString => _connectionString;

        // Use async static factory method
        private SqlServerTestStore(string name, bool useFileName = false)
        {
            Name = name;

            if (useFileName)
            {
#if NET451

                var baseDirectory = AppDomain.CurrentDomain.BaseDirectory;

#else

                var baseDirectory = AppContext.BaseDirectory;

                #endif

                _fileName = Path.Combine(baseDirectory, name + ".mdf");
            }
        }

        private static string GetScratchDbName()
        {
            string name;
            do
            {
                name = "Scratch_" + Guid.NewGuid();
            }
            while (DatabaseExists(name)
                   || DatabaseFilesExist(name));

            return name;
        }

        private SqlServerTestStore CreateShared(Action initializeDatabase)
        {
            CreateShared(typeof(SqlServerTestStore).Name + Name,
                () =>
                {
                    CreateDatabase(Name);
                    if (initializeDatabase != null)
                    {
                        initializeDatabase();
                    }
                });

            _connectionString = CreateConnectionString(Name, _fileName);
            _connection = new SqlConnection(_connectionString);

            return this;
        }

        public static void CreateDatabase(string name, string scriptPath = null, bool nonMasterScript = false, bool recreateIfAlreadyExists = false)
        {
            using (var master = new SqlConnection(CreateConnectionString("master", false)))
            {
                GetExecutionStrategy().Execute(connection =>
                    {
                        if (connection.State != ConnectionState.Closed)
                        {
                            connection.Close();
                        }
                        connection.Open();
                    }, master);

                using (var command = master.CreateCommand())
                {
                    command.CommandTimeout = CommandTimeout;

                    var exists = DatabaseExists(name);
                    if (exists)
                    {
                        if (recreateIfAlreadyExists)
                        {
                            // if scriptPath is non-null assume that the script will handle dropping DB
                            if (scriptPath == null
                                || nonMasterScript)
                            {
                                command.CommandText = GetDeleteDatabaseSql(name);

                                command.ExecuteNonQuery();
                            }
                            exists = false;
                        }
                        else
                        {
                            Clean(name);
                        }
                    }

                    try
                    {
                        if (!exists
                            && (scriptPath == null || nonMasterScript))
                        {
                            command.CommandText = GetCreateDatabaseStatement(name, null);

                            command.ExecuteNonQuery();

                            using (var newConnection = new SqlConnection(CreateConnectionString(name)))
                            {
                                WaitForExists(newConnection);
                            }
                        }

                        if (scriptPath != null)
                        {
                            // HACK: Probe for script file as current dir
                            // is different between k build and VS run.
                            if (File.Exists(@"..\..\" + scriptPath))
                            {
                                //executing in VS - so path is relative to bin\<config> dir
                                scriptPath = @"..\..\" + scriptPath;
                            }
                            else
                            {
                                scriptPath = Path.Combine(BaseDirectory, scriptPath);
                            }

                            if (nonMasterScript)
                            {
                                using (var connection = new SqlConnection(CreateConnectionString(name)))
                                {
                                    GetExecutionStrategy().Execute(state =>
                                        {
                                            if (state.connection.State != ConnectionState.Closed)
                                            {
                                                state.connection.Close();
                                            }
                                            state.connection.Open();

                                            using (var transaction = state.connection.BeginTransaction())
                                            {
                                                using (var nonMasterCommand = state.connection.CreateCommand())
                                                {
                                                    nonMasterCommand.CommandTimeout = CommandTimeout;
                                                    nonMasterCommand.Transaction = transaction;
                                                    ExecuteScript(state.scriptPath, nonMasterCommand);
                                                    transaction.Commit();
                                                }
                                            }
                                        }, new { connection, scriptPath });
                                }
                            }
                            else
                            {
                                ExecuteScript(scriptPath, command);
                            }
                        }
                    }
                    catch (Exception)
                    {
                        if (!exists)
                        {
                            try
                            {
                                GetExecutionStrategy().Execute(connection =>
                                    {
                                        if (connection.State != ConnectionState.Open)
                                        {
                                            if (connection.State != ConnectionState.Closed)
                                            {
                                                connection.Close();
                                            }
                                            connection.Open();
                                        }
                                        using (var dropCommand = connection.CreateCommand())
                                        {
                                            dropCommand.CommandTimeout = CommandTimeout;
                                            dropCommand.CommandText = GetDeleteDatabaseSql(name);
                                            dropCommand.ExecuteNonQuery();
                                        }
                                    }, master);
                            }
                            catch (Exception)
                            {
                            }
                        }
                        throw;
                    }
                }
            }
        }

        private static void ExecuteScript(string scriptPath, SqlCommand scriptCommand)
        {
            var script = File.ReadAllText(scriptPath);
            foreach (var batch in new Regex("^GO", RegexOptions.IgnoreCase | RegexOptions.Multiline, TimeSpan.FromMilliseconds(1000.0))
                .Split(script).Where(b => !string.IsNullOrEmpty(b)))
            {
                scriptCommand.CommandText = batch;

                scriptCommand.ExecuteNonQuery();
            }
        }

        private static Task WaitForExistsAsync(SqlConnection connection)
            => GetExecutionStrategy().ExecuteAsync(
                async (connectionScoped, ct) =>
                    {
                        var retryCount = 0;
                        while (true)
                        {
                            try
                            {
                                await connectionScoped.OpenAsync(ct);

                                connectionScoped.Close();
                                return;
                            }
                            catch (SqlException e)
                            {
                                if (++retryCount >= 30
                                    || (e.Number != 233 && e.Number != -2 && e.Number != 4060 && e.Number != 1832 && e.Number != 5120))
                                {
                                    throw;
                                }

                                SqlConnection.ClearPool(connectionScoped);

                                await Task.Delay(100, ct);
                            }
                        }
                    }, connection, CancellationToken.None);

        private static void WaitForExists(SqlConnection connection)
            => GetExecutionStrategy().Execute(
                connectionScoped =>
                    {
                        var retryCount = 0;
                        while (true)
                        {
                            try
                            {
                                if (connectionScoped.State != ConnectionState.Closed)
                                {
                                    connectionScoped.Close();
                                }
                                connectionScoped.Open();
                                connectionScoped.Close();
                                return;
                            }
                            catch (SqlException e)
                            {
                                if (++retryCount >= 30
                                    || (e.Number != 233 && e.Number != -2 && e.Number != 4060 && e.Number != 1832 && e.Number != 5120))
                                {
                                    throw;
                                }

                                SqlConnection.ClearPool(connectionScoped);

                                Thread.Sleep(100);
                            }
                        }
                    }, connection);

        private async Task<SqlServerTestStore> CreateTransientAsync(bool createDatabase)
        {
            _connectionString = CreateConnectionString(Name, _fileName);
            _connection = new SqlConnection(_connectionString);

            var exists = DatabaseExists(Name);
            if (createDatabase)
            {
                if (!exists)
                {
                    using (var master = new SqlConnection(CreateConnectionString("master")))
                    {
                        await GetExecutionStrategy().ExecuteAsync(async (connection, ct) =>
                            {
                                await connection.OpenAsync(ct);
                                using (var command = connection.CreateCommand())
                                {
                                    command.CommandTimeout = CommandTimeout;
                                    command.CommandText = GetCreateDatabaseStatement(Name, _fileName);

                                    await command.ExecuteNonQueryAsync(ct);
                                }
                            }, master);
                    }

                    await WaitForExistsAsync(_connection);
                }
                else
                {
                    Clean(Name);
                }

                await GetExecutionStrategy().ExecuteAsync((connection, ct) => connection.OpenAsync(ct), _connection);
            }
            else if (exists)
            {
                DeleteDatabase(Name);
            }

            _deleteDatabase = true;
            return this;
        }

        private SqlServerTestStore CreateTransient(bool createDatabase)
        {
            _connectionString = CreateConnectionString(Name, _fileName);
            _connection = new SqlConnection(_connectionString);

            var exists = DatabaseExists(Name);
            if (createDatabase)
            {
                if (!exists)
                {
                    using (var master = new SqlConnection(CreateConnectionString("master")))
                    {
                        GetExecutionStrategy().Execute(connection =>
                            {
                                if (connection.State != ConnectionState.Closed)
                                {
                                    connection.Close();
                                }
                                connection.Open();
                                using (var command = connection.CreateCommand())
                                {
                                    command.CommandTimeout = CommandTimeout;
                                    command.CommandText = GetCreateDatabaseStatement(Name, _fileName);

                                    command.ExecuteNonQuery();
                                }
                            }, master);
                    }

                    WaitForExists(_connection);
                }
                else
                {
                    Clean(Name);
                }

                GetExecutionStrategy().Execute(connection => connection.Open(), _connection);
            }
            else if (exists)
            {
                DeleteDatabase(Name);
            }

            _deleteDatabase = true;
            return this;
        }

        private static void Clean(string name)
        {
            var serviceProvider = new ServiceCollection()
                .AddEntityFrameworkSqlServer()
                .BuildServiceProvider();

            var options = new DbContextOptionsBuilder()
                .UseSqlServer(CreateConnectionString(name), b => b.ApplyConfiguration())
                .EnableSensitiveDataLogging()
                .UseInternalServiceProvider(serviceProvider)
                .Options;

            using (var context = new DbContext(options))
            {
                context.Database.EnsureClean();
            }
        }

        private static string GetCreateDatabaseStatement(string name, string fileName)
        {
            var result = $"CREATE DATABASE [{name}]";

            if (TestEnvironment.IsSqlAzure)
            {
                var elasticGroupName = TestEnvironment.ElasticGroupName;
                result += Environment.NewLine +
                          (string.IsNullOrEmpty(elasticGroupName)
                              ? " ( Edition = 'basic' )"
                              : $" ( SERVICE_OBJECTIVE = ELASTIC_POOL ( name = {elasticGroupName} ) )");
            }
            else
            {
                if (!string.IsNullOrEmpty(fileName))
                {
                    var logFileName = Path.ChangeExtension(fileName, ".ldf");
                    result += Environment.NewLine +
                              $" ON (NAME = '{name}', FILENAME = '{fileName}')" +
                              $" LOG ON (NAME = '{name}_log', FILENAME = '{logFileName}')";
                }
            }
            return result;
        }

        private static bool DatabaseExists(string name)
        {
            using (var master = new SqlConnection(CreateConnectionString("master")))
            {
                return GetExecutionStrategy().Execute(connection =>
                    {
                        if (connection.State != ConnectionState.Closed)
                        {
                            connection.Close();
                        }
                        connection.Open();

                        using (var command = connection.CreateCommand())
                        {
                            command.CommandTimeout = CommandTimeout;
                            command.CommandText = $@"SELECT COUNT(*) FROM sys.databases WHERE name = N'{name}'";

                            return (int)command.ExecuteScalar() > 0;
                        }
                    }, master);
            }
        }

        private static bool TablesExist(string name)
        {
            using (var c = new SqlConnection(CreateConnectionString(name)))
            {
                return GetExecutionStrategy().Execute(connection =>
                    {
                        if (connection.State != ConnectionState.Closed)
                        {
                            connection.Close();
                        }
                        connection.Open();

                        using (var command = connection.CreateCommand())
                        {
                            command.CommandTimeout = CommandTimeout;
                            command.CommandText = @"SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_SCHEMA != 'sys'";

                            var result = (int)command.ExecuteScalar() > 0;

                            connection.Close();

                            SqlConnection.ClearAllPools();

                            return result;
                        }
                    }, c);
            }
        }

        private static bool DatabaseFilesExist(string name)
        {
            var userFolder = Environment.GetEnvironmentVariable("USERPROFILE") ?? Environment.GetEnvironmentVariable("HOME");
            return userFolder != null
                   && (File.Exists(Path.Combine(userFolder, name + ".mdf"))
                       || File.Exists(Path.Combine(userFolder, name + "_log.ldf")));
        }

        private void DeleteDatabase(string name)
        {
            using (var master = new SqlConnection(CreateConnectionString("master")))
            {
                GetExecutionStrategy().Execute(connection =>
                    {
                        if (connection.State != ConnectionState.Open)
                        {
                            if (connection.State != ConnectionState.Closed)
                            {
                                connection.Close();
                            }
                            connection.Open();
                        }

                        using (var command = connection.CreateCommand())
                        {
                            command.CommandTimeout = CommandTimeout; // Query will take a few seconds if (and only if) there are active connections

                            command.CommandText = GetDeleteDatabaseSql(name);

                            command.ExecuteNonQuery();
                        }

                        SqlConnection.ClearPool(connection);
                    }, master);
            }
        }

        private static string GetDeleteDatabaseSql(string name)
            // SET SINGLE_USER will close any open connections that would prevent the drop
            => string.Format(@"IF EXISTS (SELECT * FROM sys.databases WHERE name = N'{0}')
                                          BEGIN
                                              ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                                              DROP DATABASE [{0}];
                                          END", name);

        public static IExecutionStrategy GetExecutionStrategy()
            => TestEnvironment.IsSqlAzure ? new TestSqlAzureExecutionStrategy() : (IExecutionStrategy)NoopExecutionStrategy.Instance;

        public override DbConnection Connection => _connection;

        public override DbTransaction Transaction => null;

        public async Task<T> ExecuteScalarAsync<T>(string sql, CancellationToken cancellationToken, params object[] parameters)
        {
            using (var command = CreateCommand(sql, parameters))
            {
                return (T)await command.ExecuteScalarAsync(cancellationToken);
            }
        }

        public int ExecuteNonQuery(string sql, params object[] parameters)
        {
            using (var command = CreateCommand(sql, parameters))
            {
                return command.ExecuteNonQuery();
            }
        }

        public Task<int> ExecuteNonQueryAsync(string sql, params object[] parameters)
        {
            using (var command = CreateCommand(sql, parameters))
            {
                return command.ExecuteNonQueryAsync();
            }
        }

        public async Task<IEnumerable<T>> QueryAsync<T>(string sql, params object[] parameters)
        {
            using (var command = CreateCommand(sql, parameters))
            {
                using (var dataReader = await command.ExecuteReaderAsync())
                {
                    var results = Enumerable.Empty<T>();

                    while (await dataReader.ReadAsync())
                    {
                        results = results.Concat(new[] { await dataReader.GetFieldValueAsync<T>(0) });
                    }

                    return results;
                }
            }
        }

        private DbCommand CreateCommand(string commandText, object[] parameters)
        {
            var command = _connection.CreateCommand();

            command.CommandText = commandText;
            command.CommandTimeout = CommandTimeout;

            for (var i = 0; i < parameters.Length; i++)
            {
                command.Parameters.AddWithValue("p" + i, parameters[i]);
            }

            return command;
        }

        public override void Dispose()
        {
            _connection.Dispose();

            if (_deleteDatabase)
            {
                DeleteDatabase(Name);
            }
        }

        public static string CreateConnectionString(string name)
            => CreateConnectionString(name, null, new Random().Next(0, 2) == 1);

        public static string CreateConnectionString(string name, string fileName)
            => CreateConnectionString(name, fileName, new Random().Next(0, 2) == 1);

        private static string CreateConnectionString(string name, bool multipleActiveResultSets)
            => CreateConnectionString(name, null, multipleActiveResultSets);

        private static string CreateConnectionString(string name, string fileName, bool multipleActiveResultSets)
        {
            var builder = new SqlConnectionStringBuilder(TestEnvironment.DefaultConnection)
            {
                MultipleActiveResultSets = multipleActiveResultSets,
                InitialCatalog = name
            };
            if (fileName != null)
            {
                builder.AttachDBFilename = fileName;
            }

            return builder.ToString();
        }
    }
}
