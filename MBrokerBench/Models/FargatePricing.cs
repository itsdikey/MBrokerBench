using System;
using System.Collections.Generic;

namespace MBrokerBench.Models
{
    /// <summary>
    /// Regular AWS Fargate (Linux/x86) on-demand pricing snapshot used for the
    /// independent Fargate cost metric. Prices are for the US East (Ohio) region
    /// (us-east-2) and are dated 2026-09-03.
    /// </summary>
    /// <remarks>
    /// Monetary math uses System.Decimal exclusively so per-task bills never suffer
    /// binary floating-point rounding surprises. Unit prices are hourly; monetary
    /// values are derived as <c>seconds * hourlyRate / 3600</c> (single decimal
    /// division at the end) so intermediate accumulations stay exact.
    ///
    /// Billing semantics implemented across Consumer/ConsumerGroup:
    /// <list type="bullet">
    ///   <item>A task is billed from simulated provisioning (creation) until simulated
    ///   termination (removal). All lifecycle states (Booting/Syncing/Running) are
    ///   billed identically, by presence only; no image-download or boot duration is
    ///   fabricated.</item>
    ///   <item>Every simulated second in which the task is alive accrues one billable
    ///   second on a per-task ledger (never a profile-count tick multiplication).</item>
    ///   <item>Each task is subject to a 60-second minimum: on settlement (removal, or
    ///   the end-of-run policy below) the charge uses max(60, elapsed seconds).</item>
    ///   <item>End-of-run policy: a task still alive at the end of a run is treated as
    ///   terminated exactly at the final tick (end of run). Its bill is settled with the
    ///   same max(60, elapsed) rule and is therefore exposed as its accrued bill on the
    ///   final tick, making the final per-tick accrued cost equal the final aggregate.</item>
    /// </list>
    /// </remarks>
    public static class FargatePricing
    {
        /// <summary>AWS region code for US East (Ohio).</summary>
        public const string Region = "us-east-2";

        /// <summary>Fargate platform used for this metric.</summary>
        public const string Platform = "Linux/x86_64";

        /// <summary>Effective date of the price snapshot.</summary>
        public const string SnapshotDate = "2026-09-03";

        /// <summary>USD per vCPU per hour.</summary>
        public const decimal VCpuPerHour = 0.04048m;

        /// <summary>USD per GiB of memory per hour.</summary>
        public const decimal MemoryPerGiBPerHour = 0.004445m;

        /// <summary>USD per GiB of extra ephemeral storage per hour.</summary>
        public const decimal ExtraEphemeralStoragePerGiBPerHour = 0.000111m;

        /// <summary>GiB of ephemeral storage included with every task at no extra charge.</summary>
        public const decimal IncludedEphemeralStorageGiB = 20m;

        /// <summary>Seconds in one hour (used to convert the hourly rates).</summary>
        public const decimal SecondsPerHour = 3600m;

        /// <summary>
        /// Minimum billable lifetime per task, in seconds. Fargate bills in one-second
        /// increments with a one-minute minimum, so a task is charged for at least this
        /// many seconds no matter how briefly it ran.
        /// </summary>
        public const int MinimumBillableSeconds = 60;

        private static readonly IReadOnlyDictionary<string, FargateTaskDefinition> DefinitionsMap =
            new Dictionary<string, FargateTaskDefinition>(StringComparer.Ordinal)
            {
                // All modeled tasks use the default 20 GiB ephemeral storage (the largest
                // task below needs only 4 GiB), so extra ephemeral storage is 0 GiB and
                // its cost term is exactly zero.
                [nameof(ConsumerProfiles.Small)] = new FargateTaskDefinition("Small", 0.5m, 1m, 0m),
                [nameof(ConsumerProfiles.Medium)] = new FargateTaskDefinition("Medium", 1m, 2m, 0m),
                [nameof(ConsumerProfiles.Large)] = new FargateTaskDefinition("Large", 2m, 4m, 0m),
            };

        /// <summary>Known Fargate task definitions, keyed by consumer profile name.</summary>
        public static IReadOnlyDictionary<string, FargateTaskDefinition> Definitions => DefinitionsMap;

        /// <summary>Resolves the Fargate task definition for a consumer profile name.</summary>
        /// <exception cref="KeyNotFoundException">Thrown when no task definition exists for the profile.</exception>
        public static FargateTaskDefinition GetTaskDefinition(string profileName)
        {
            if (DefinitionsMap.TryGetValue(profileName, out FargateTaskDefinition? definition))
            {
                return definition;
            }

            throw new KeyNotFoundException(
                $"No Fargate task definition exists for consumer profile '{profileName}'. " +
                $"Known task sizes: {string.Join(", ", DefinitionsMap.Keys)}.");
        }

        /// <summary>
        /// Monetary cost (USD) for a task that runs <paramref name="billableSeconds"/>
        /// seconds at the hourly rate of <paramref name="definition"/>.
        /// </summary>
        public static decimal CostForSeconds(FargateTaskDefinition definition, decimal billableSeconds)
        {
            if (definition == null) throw new ArgumentNullException(nameof(definition));
            return definition.HourlyRate * billableSeconds / SecondsPerHour;
        }
    }

    /// <summary>
    /// Resource sizing and hourly on-demand rate of one Fargate task size.
    /// </summary>
    public sealed class FargateTaskDefinition
    {
        public FargateTaskDefinition(string name, decimal vCpus, decimal memoryGiB, decimal extraEphemeralStorageGiB)
        {
            Name = name;
            VCpus = vCpus;
            MemoryGiB = memoryGiB;
            ExtraEphemeralStorageGiB = extraEphemeralStorageGiB;
        }

        /// <summary>Task size name (matches the consumer profile name it bills for).</summary>
        public string Name { get; }

        /// <summary>vCPUs provisioned for the task.</summary>
        public decimal VCpus { get; }

        /// <summary>Memory (GiB) provisioned for the task.</summary>
        public decimal MemoryGiB { get; }

        /// <summary>
        /// Extra ephemeral storage (GiB) above the included 20 GiB. Zero for all modeled
        /// tasks, so this never contributes to cost.
        /// </summary>
        public decimal ExtraEphemeralStorageGiB { get; }

        /// <summary>USD per hour for one task of this size.</summary>
        public decimal HourlyRate =>
            VCpus * FargatePricing.VCpuPerHour
            + MemoryGiB * FargatePricing.MemoryPerGiBPerHour
            + ExtraEphemeralStorageGiB * FargatePricing.ExtraEphemeralStoragePerGiBPerHour;
    }

    /// <summary>
    /// Task-level billing ledger for a single Fargate task (one simulated consumer).
    /// It records the task's billable elapsed seconds while alive and, at settlement,
    /// applies the 60-second minimum to produce the final charge.
    /// </summary>
    public sealed class FargateTaskBill
    {
        private readonly FargateTaskDefinition _definition;

        public FargateTaskBill(FargateTaskDefinition definition)
        {
            _definition = definition ?? throw new ArgumentNullException(nameof(definition));
        }

        /// <summary>Elapsed seconds accrued while the task is alive (unsettled).</summary>
        public decimal AccruedSeconds { get; private set; }

        /// <summary>True once the task bill has been settled.</summary>
        public bool IsSettled { get; private set; }

        /// <summary>Billable seconds used for the settled charge (includes the 60-second minimum).</summary>
        public decimal SettledSeconds { get; private set; }

        /// <summary>Settled monetary charge in USD.</summary>
        public decimal SettledCostUsd { get; private set; }

        public string TaskName => _definition.Name;

        public decimal VCpus => _definition.VCpus;

        public decimal MemoryGiB => _definition.MemoryGiB;

        public decimal HourlyRate => _definition.HourlyRate;

        /// <summary>
        /// Cost accrued for the elapsed seconds so far, without any settlement
        /// minimum top-up. Once settled this is superseded by <see cref="SettledCostUsd"/>.
        /// </summary>
        public decimal AccruedCostUsd => FargatePricing.CostForSeconds(_definition, AccruedSeconds);

        /// <summary>
        /// Seconds that would be billed if this task were settled now
        /// (elapsed, or the 60-second minimum, whichever is larger).
        /// </summary>
        public decimal EffectiveBillableSeconds =>
            IsSettled ? SettledSeconds : Math.Max(FargatePricing.MinimumBillableSeconds, AccruedSeconds);

        /// <summary>Accrues <paramref name="seconds"/> of billable lifetime to this task.</summary>
        public void AccrueSeconds(decimal seconds)
        {
            if (IsSettled)
            {
                return;
            }
            AccruedSeconds += seconds;
        }

        /// <summary>
        /// Settles the task: applies the 60-second minimum and records the final charge.
        /// A task is settled at most once; further accruals are ignored after settlement.
        /// </summary>
        public void Settle()
        {
            if (IsSettled)
            {
                return;
            }
            SettledSeconds = Math.Max(FargatePricing.MinimumBillableSeconds, AccruedSeconds);
            SettledCostUsd = FargatePricing.CostForSeconds(_definition, SettledSeconds);
            IsSettled = true;
        }
    }
}
