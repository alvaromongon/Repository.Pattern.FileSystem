using Repository.Pattern.FileSystem.IntegrationTests.DomainModel;

namespace Repository.Pattern.FileSystem.IntegrationTests.Repository
{
    public class DomainModelClassRepository : FileSystemRepository<DomainModelClass>
    {
        public DomainModelClassRepository(IRepositoryConfiguration repositoryConfiguration)
            : base(repositoryConfiguration) { }
    }
}
