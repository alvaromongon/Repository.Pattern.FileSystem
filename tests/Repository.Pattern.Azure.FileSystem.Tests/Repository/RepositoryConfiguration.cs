using System;

namespace Repository.Pattern.FileSystem.IntegrationTests.Repository
{
    public class RepositoryConfiguration : IRepositoryConfiguration
    {
        public string DirectoryPath => Environment.CurrentDirectory;
    }
}
