using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Repository.Pattern.Abstractions;
using Repository.Pattern.Abstractions.Batches;
using Repository.Pattern.Abstractions.Exceptions;
using Repository.Pattern.Abstractions.Exceptions.Models;

namespace Repository.Pattern.FileSystem
{
    /// <summary>
    /// File system implementation of the repository pattern using a ConcurrentDictionary
    /// </summary>
    /// <typeparam name="T">A Mode object not related with Table storage at all</typeparam>
    public abstract class FileSystemRepository<TDomainModel> : IRepository<TDomainModel> where TDomainModel : class, IDomainModel, new()
    {
        private const string FileNamePatternSuffix = "_Repository.json";

        private readonly IRepositoryConfiguration _configuration;

        public FileSystemRepository(IRepositoryConfiguration configuration)
        {
            _configuration = configuration;

            Directory.CreateDirectory(_configuration.DirectoryPath);
        }

        public Task<IEnumerable<TDomainModel>> GetAllAsync()
        {
            var result = new List<TDomainModel>();
            var files = Directory.GetFiles(_configuration.DirectoryPath, $"*{FileNamePatternSuffix}", SearchOption.TopDirectoryOnly);

            if (files != null)
            {
                foreach (var file in files)
                {
                    result.AddRange(Read(file));
                }
            }

            return Task.FromResult(result as IEnumerable<TDomainModel>);
        }

        public Task<IEnumerable<TDomainModel>> GetAllAsync(string partitionKey)
        {
            var filePath = BuildFilePath(partitionKey);
            var partitionKeyValues = Read(filePath, partitionKey);

            var result = partitionKeyValues == null
                ? new List<TDomainModel>()
                : partitionKeyValues as IEnumerable<TDomainModel>;

            return Task.FromResult(result);
        }

        public async Task<TDomainModel> GetAsync(string partitionKey, string rowKey)
        {
            var partitionKeyValues = await GetAllAsync(partitionKey).ConfigureAwait(false);
            var rowKeyValue = partitionKeyValues.FirstOrDefault(v => v.RowKey.Equals(rowKey, StringComparison.InvariantCultureIgnoreCase));

            if (rowKeyValue == null)
            {
                throw new DoesNotExistsException($"{partitionKey}.{rowKey}");
            }

            return rowKeyValue;
        }

        public async Task<TDomainModel> AddAsync(TDomainModel domainModel)
        {
            var partitionKeyValues = (await GetAllAsync(domainModel.PartitionKey).ConfigureAwait(false))
                                        .ToList();

            if (partitionKeyValues.Any(v => v.RowKey.Equals(domainModel.RowKey, StringComparison.InvariantCultureIgnoreCase)))
            {
                throw new AlreadyExistsException(nameof(AddAsync))
                {
                    DomainModelUids = BuildDomainModelUids(domainModel)
                };
            }

            partitionKeyValues.Add(domainModel);

            Write(partitionKeyValues);

            return domainModel;
        }

        public async Task AddBatchAsync(IEnumerable<TDomainModel> domainModelEnumerable, BatchOperationOptions options = null)
        {
            var domainModels = domainModelEnumerable.ToList();

            Func<TDomainModel, Task<TDomainModel>> operation = (dm) => AddOrUpdateAsync(dm);

            if (options != null)
            {
                switch (options.BatchInsertMethod)
                {
                    case BatchInsertMethod.Insert:
                        var domainModelsByPartitionkey = domainModels.GroupBy(dm => dm.PartitionKey);
                        var existingDomainModels = new List<TDomainModel>();

                        foreach (var domainModelByPartitionKey in domainModelsByPartitionkey)
                        {
                            var domainModelsInDb = (await GetAllAsync(domainModelByPartitionKey.Key).ConfigureAwait(false))
                                        .ToList();
                            existingDomainModels.AddRange(domainModelsInDb.Where(dm => domainModelByPartitionKey.Any(dmbpk => dm.RowKey == dmbpk.RowKey)));
                        }

                        if (existingDomainModels.Any())
                        {
                            throw new AlreadyExistsException(nameof(AddBatchAsync))
                            {
                                DomainModelUids = BuildDomainModelUids(existingDomainModels.ToArray())
                            };
                        }

                        operation = (dm) => AddAsync(dm);
                        break;
                        // We will implement both the same way, like an upsert
                        //case BatchInsertMethod.InsertOrMerge:
                        //case BatchInsertMethod.InsertOrReplace:
                }
            }

            foreach (var domainModel in domainModelEnumerable)
            {
                await operation(domainModel);
            }

            return;
        }

        public async Task<TDomainModel> AddOrUpdateAsync(TDomainModel domainModel)
        {
            var partitionKeyValues = (await GetAllAsync(domainModel.PartitionKey).ConfigureAwait(false))
                                        .ToList();

            var value = partitionKeyValues.FirstOrDefault(v => v.RowKey.Equals(domainModel.RowKey, StringComparison.InvariantCultureIgnoreCase));
            if (value == null)
            {
                partitionKeyValues.Add(domainModel);
            }
            else
            {
                partitionKeyValues[partitionKeyValues.IndexOf(value)] = domainModel;
            }

            Write(partitionKeyValues);

            return domainModel;
        }

        public async Task<TDomainModel> UpdateAsync(TDomainModel domainModel)
        {
            var partitionKeyValues = (await GetAllAsync(domainModel.PartitionKey).ConfigureAwait(false))
                            .ToList();

            var value = partitionKeyValues.FirstOrDefault(v => v.RowKey.Equals(domainModel.RowKey, StringComparison.InvariantCultureIgnoreCase));
            if (value == null)
            {
                throw new DoesNotExistsException(nameof(AddOrUpdateAsync))
                {
                    DomainModelUids = BuildDomainModelUids(domainModel)
                };
            }
            else
            {
                partitionKeyValues[partitionKeyValues.IndexOf(value)] = domainModel;
            }

            Write(partitionKeyValues);

            return domainModel;
        }

        public Task DeleteAllAsync(string partitionKey)
        {
            var filePath = BuildFilePath(partitionKey);
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }

            return Task.CompletedTask;
        }

        public Task<TDomainModel> DeleteAsync(TDomainModel domainModel)
        {
            return DeleteAsync(domainModel.PartitionKey, domainModel.RowKey);
        }

        public async Task<TDomainModel> DeleteAsync(string partitionKey, string rowKey)
        {
            var partitionKeyValues = (await GetAllAsync(partitionKey).ConfigureAwait(false))
                .ToList();
            var rowKeyValue = partitionKeyValues.FirstOrDefault(v => v.RowKey.Equals(rowKey, StringComparison.InvariantCultureIgnoreCase));

            if (rowKeyValue == null)
            {
                throw new DoesNotExistsException($"{partitionKey}.{rowKey}");
            }

            partitionKeyValues.Remove(rowKeyValue);

            if (partitionKeyValues.Count == 0)
            {
                await DeleteAllAsync(partitionKey).ConfigureAwait(false);
            }
            else
            {
                Write(partitionKeyValues);
            }

            return rowKeyValue;
        }

        private T GetValue<T>(string key, ConcurrentDictionary<string, T> dictionary,
            string callerName = nameof(GetValue), bool throwException = true)
        {
            var exists = dictionary.TryGetValue(key, out T entries);

            if (throwException && !exists)
            {
                throw new DoesNotExistsException($"{callerName}: {key}");
            }

            return entries;
        }

        private DomainModelUid[] BuildDomainModelUids(params IDomainModel[] domainModels)
        {
            return domainModels.Select(dm => new DomainModelUid()
            {
                PartitionKey = dm.PartitionKey,
                RowKey = dm.RowKey
            }).ToArray();
        }

        private List<TDomainModel> Read(string filePath, string key = null)
        {
            List<TDomainModel> result = new List<TDomainModel>();
            if (File.Exists(filePath))
            {
                using (var file = File.OpenText(filePath))
                {
                    var serializer = new JsonSerializer();
                    result = (List<TDomainModel>)serializer.Deserialize(file, typeof(List<TDomainModel>));
                }
            }

            return result;
        }

        private void Write(List<TDomainModel> data)
        {
            var dataGrouped = data.GroupBy(d => d.PartitionKey);

            foreach (var dg in dataGrouped)
            {
                var filePath = BuildFilePath(dg.Key);
                File.WriteAllText(filePath, JsonConvert.SerializeObject(dg));
            }
        }

        private string BuildFilePath(string partitionKey)
        {
            return Path.Combine(_configuration.DirectoryPath, $"{partitionKey}{FileNamePatternSuffix}");
        }
    }
}
