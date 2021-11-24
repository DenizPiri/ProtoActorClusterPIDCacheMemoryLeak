using Proto;
using Proto.Cluster;
using Proto.Cluster.Partition;
using Proto.Cluster.Testing;
using Proto.Remote.GrpcCore;

ClusterConfig GetClusterConfig() => ClusterConfig
    .Setup(
        "MyCluster",
        new TestProvider(new TestProviderOptions(), new InMemAgent()),
        new PartitionIdentityLookup(TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(500)))
    .WithClusterKind("thing", Props.FromProducer(() => new ThingActor()));

var system = new ActorSystem()
    .WithRemote(GrpcCoreRemoteConfig.BindToLocalhost())
    .WithCluster(GetClusterConfig());

await system.Cluster().StartMemberAsync();

for (int i = 0; i < 1000000; i++)
{
    if (i % 10000 == 0)
    {
        long memorySize = GC.GetTotalMemory(forceFullCollection: true);
        Console.WriteLine($"{i} spawned. Process Count: {system.ProcessRegistry.ProcessCount} - GC Memory Size: {memorySize / 1024 / 1024} MB");
    }
    
    await system.Cluster().RequestAsync<object>(
        $"{Guid.NewGuid()}",
        "thing",
        new SomeMessage(),
        CancellationToken.None);
}

class ThingActor : IActor
{
    Task IActor.ReceiveAsync(IContext context)
    {
        if (context.Message is SomeMessage)
        {
            context.Respond(context.Message);
            // This should kill self and clean all memory.
            // But the PIDCache holds all PIDs and each PID holds process
            // reference. So all the ActorProcess and RemoteProcess references
            // forevers stays in memory.
            context.Stop(context.Self);
        }
        return Task.CompletedTask;
    }
}

record SomeMessage;
