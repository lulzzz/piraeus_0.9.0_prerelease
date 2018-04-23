#if !EXCLUDE_CODEGEN
#pragma warning disable 162
#pragma warning disable 219
#pragma warning disable 414
#pragma warning disable 649
#pragma warning disable 693
#pragma warning disable 1591
#pragma warning disable 1998
[assembly: global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0")]
[assembly: global::Orleans.CodeGeneration.OrleansCodeGenerationTargetAttribute("Piraeus.Grains, Version=0.9.3.0, Culture=neutral, PublicKeyToken=null")]
namespace Piraeus.GrainInterfaces
{
    using global::Orleans.Async;
    using global::Orleans;
    using global::System.Reflection;

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.SerializerAttribute(typeof (global::Piraeus.GrainInterfaces.AccessControlState))]
    internal class OrleansCodeGenPiraeus_GrainInterfaces_AccessControlStateSerializer
    {
        [global::Orleans.CodeGeneration.CopierMethodAttribute]
        public static global::System.Object DeepCopier(global::System.Object original, global::Orleans.Serialization.ICopyContext context)
        {
            global::Piraeus.GrainInterfaces.AccessControlState input = ((global::Piraeus.GrainInterfaces.AccessControlState)original);
            global::Piraeus.GrainInterfaces.AccessControlState result = new global::Piraeus.GrainInterfaces.AccessControlState();
            context.@RecordCopy(original, result);
            result.@Policy = (global::Capl.Authorization.AuthorizationPolicy)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@Policy, context);
            return result;
        }

        [global::Orleans.CodeGeneration.SerializerMethodAttribute]
        public static void Serializer(global::System.Object untypedInput, global::Orleans.Serialization.ISerializationContext context, global::System.Type expected)
        {
            global::Piraeus.GrainInterfaces.AccessControlState input = (global::Piraeus.GrainInterfaces.AccessControlState)untypedInput;
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Policy, context, typeof (global::Capl.Authorization.AuthorizationPolicy));
        }

        [global::Orleans.CodeGeneration.DeserializerMethodAttribute]
        public static global::System.Object Deserializer(global::System.Type expected, global::Orleans.Serialization.IDeserializationContext context)
        {
            global::Piraeus.GrainInterfaces.AccessControlState result = new global::Piraeus.GrainInterfaces.AccessControlState();
            context.@RecordObject(result);
            result.@Policy = (global::Capl.Authorization.AuthorizationPolicy)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::Capl.Authorization.AuthorizationPolicy), context);
            return (global::Piraeus.GrainInterfaces.AccessControlState)result;
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.SerializerAttribute(typeof (global::Capl.Authorization.AuthorizationPolicy))]
    internal class OrleansCodeGenCapl_Authorization_AuthorizationPolicySerializer
    {
        private static readonly global::System.Reflection.FieldInfo field2 = typeof (global::Capl.Authorization.AuthorizationPolicy).@GetTypeInfo().@GetField("<Transforms>k__BackingField", (System.@Reflection.@BindingFlags.@Instance | System.@Reflection.@BindingFlags.@NonPublic | System.@Reflection.@BindingFlags.@Public));
        private static readonly global::System.Action<global::Capl.Authorization.AuthorizationPolicy, global::Capl.Authorization.TransformCollection> setField2 = (global::System.Action<global::Capl.Authorization.AuthorizationPolicy, global::Capl.Authorization.TransformCollection>)global::Orleans.Serialization.SerializationManager.@GetReferenceSetter(field2);
        [global::Orleans.CodeGeneration.CopierMethodAttribute]
        public static global::System.Object DeepCopier(global::System.Object original, global::Orleans.Serialization.ICopyContext context)
        {
            global::Capl.Authorization.AuthorizationPolicy input = ((global::Capl.Authorization.AuthorizationPolicy)original);
            global::Capl.Authorization.AuthorizationPolicy result = new global::Capl.Authorization.AuthorizationPolicy();
            context.@RecordCopy(original, result);
            result.@Delegation = input.@Delegation;
            result.@Expression = (global::Capl.Authorization.Term)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@Expression, context);
            result.@PolicyId = (global::System.Uri)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@PolicyId, context);
            setField2(result, (global::Capl.Authorization.TransformCollection)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@Transforms, context));
            return result;
        }

        [global::Orleans.CodeGeneration.SerializerMethodAttribute]
        public static void Serializer(global::System.Object untypedInput, global::Orleans.Serialization.ISerializationContext context, global::System.Type expected)
        {
            global::Capl.Authorization.AuthorizationPolicy input = (global::Capl.Authorization.AuthorizationPolicy)untypedInput;
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Delegation, context, typeof (global::System.Boolean));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Expression, context, typeof (global::Capl.Authorization.Term));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@PolicyId, context, typeof (global::System.Uri));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Transforms, context, typeof (global::Capl.Authorization.TransformCollection));
        }

        [global::Orleans.CodeGeneration.DeserializerMethodAttribute]
        public static global::System.Object Deserializer(global::System.Type expected, global::Orleans.Serialization.IDeserializationContext context)
        {
            global::Capl.Authorization.AuthorizationPolicy result = new global::Capl.Authorization.AuthorizationPolicy();
            context.@RecordObject(result);
            result.@Delegation = (global::System.Boolean)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Boolean), context);
            result.@Expression = (global::Capl.Authorization.Term)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::Capl.Authorization.Term), context);
            result.@PolicyId = (global::System.Uri)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Uri), context);
            setField2(result, (global::Capl.Authorization.TransformCollection)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::Capl.Authorization.TransformCollection), context));
            return (global::Capl.Authorization.AuthorizationPolicy)result;
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.SerializerAttribute(typeof (global::Capl.Authorization.TransformCollection))]
    internal class OrleansCodeGenCapl_Authorization_TransformCollectionSerializer
    {
        private static readonly global::System.Reflection.FieldInfo field0 = typeof (global::Capl.Authorization.TransformCollection).@GetTypeInfo().@GetField("transforms", (System.@Reflection.@BindingFlags.@Instance | System.@Reflection.@BindingFlags.@NonPublic | System.@Reflection.@BindingFlags.@Public));
        private static readonly global::System.Func<global::Capl.Authorization.TransformCollection, global::System.Collections.Generic.List<global::Capl.Authorization.Transform>> getField0 = (global::System.Func<global::Capl.Authorization.TransformCollection, global::System.Collections.Generic.List<global::Capl.Authorization.Transform>>)global::Orleans.Serialization.SerializationManager.@GetGetter(field0);
        private static readonly global::System.Action<global::Capl.Authorization.TransformCollection, global::System.Collections.Generic.List<global::Capl.Authorization.Transform>> setField0 = (global::System.Action<global::Capl.Authorization.TransformCollection, global::System.Collections.Generic.List<global::Capl.Authorization.Transform>>)global::Orleans.Serialization.SerializationManager.@GetReferenceSetter(field0);
        [global::Orleans.CodeGeneration.CopierMethodAttribute]
        public static global::System.Object DeepCopier(global::System.Object original, global::Orleans.Serialization.ICopyContext context)
        {
            global::Capl.Authorization.TransformCollection input = ((global::Capl.Authorization.TransformCollection)original);
            global::Capl.Authorization.TransformCollection result = new global::Capl.Authorization.TransformCollection();
            context.@RecordCopy(original, result);
            setField0(result, (global::System.Collections.Generic.List<global::Capl.Authorization.Transform>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(getField0(input), context));
            return result;
        }

        [global::Orleans.CodeGeneration.SerializerMethodAttribute]
        public static void Serializer(global::System.Object untypedInput, global::Orleans.Serialization.ISerializationContext context, global::System.Type expected)
        {
            global::Capl.Authorization.TransformCollection input = (global::Capl.Authorization.TransformCollection)untypedInput;
            global::Orleans.Serialization.SerializationManager.@SerializeInner(getField0(input), context, typeof (global::System.Collections.Generic.List<global::Capl.Authorization.Transform>));
        }

        [global::Orleans.CodeGeneration.DeserializerMethodAttribute]
        public static global::System.Object Deserializer(global::System.Type expected, global::Orleans.Serialization.IDeserializationContext context)
        {
            global::Capl.Authorization.TransformCollection result = new global::Capl.Authorization.TransformCollection();
            context.@RecordObject(result);
            setField0(result, (global::System.Collections.Generic.List<global::Capl.Authorization.Transform>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Collections.Generic.List<global::Capl.Authorization.Transform>), context));
            return (global::Capl.Authorization.TransformCollection)result;
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.SerializerAttribute(typeof (global::Piraeus.GrainInterfaces.AuditConfigState))]
    internal class OrleansCodeGenPiraeus_GrainInterfaces_AuditConfigStateSerializer
    {
        [global::Orleans.CodeGeneration.CopierMethodAttribute]
        public static global::System.Object DeepCopier(global::System.Object original, global::Orleans.Serialization.ICopyContext context)
        {
            global::Piraeus.GrainInterfaces.AuditConfigState input = ((global::Piraeus.GrainInterfaces.AuditConfigState)original);
            global::Piraeus.GrainInterfaces.AuditConfigState result = new global::Piraeus.GrainInterfaces.AuditConfigState();
            context.@RecordCopy(original, result);
            result.@ConnectionString = input.@ConnectionString;
            result.@Tablename = input.@Tablename;
            return result;
        }

        [global::Orleans.CodeGeneration.SerializerMethodAttribute]
        public static void Serializer(global::System.Object untypedInput, global::Orleans.Serialization.ISerializationContext context, global::System.Type expected)
        {
            global::Piraeus.GrainInterfaces.AuditConfigState input = (global::Piraeus.GrainInterfaces.AuditConfigState)untypedInput;
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@ConnectionString, context, typeof (global::System.String));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Tablename, context, typeof (global::System.String));
        }

        [global::Orleans.CodeGeneration.DeserializerMethodAttribute]
        public static global::System.Object Deserializer(global::System.Type expected, global::Orleans.Serialization.IDeserializationContext context)
        {
            global::Piraeus.GrainInterfaces.AuditConfigState result = new global::Piraeus.GrainInterfaces.AuditConfigState();
            context.@RecordObject(result);
            result.@ConnectionString = (global::System.String)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.String), context);
            result.@Tablename = (global::System.String)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.String), context);
            return (global::Piraeus.GrainInterfaces.AuditConfigState)result;
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.SerializerAttribute(typeof (global::Piraeus.GrainInterfaces.ResourceState))]
    internal class OrleansCodeGenPiraeus_GrainInterfaces_ResourceStateSerializer
    {
        private static readonly global::System.Reflection.FieldInfo field6 = typeof (global::Piraeus.GrainInterfaces.ResourceState).@GetTypeInfo().@GetField("Subscriptions", (System.@Reflection.@BindingFlags.@Instance | System.@Reflection.@BindingFlags.@NonPublic | System.@Reflection.@BindingFlags.@Public));
        private static readonly global::System.Func<global::Piraeus.GrainInterfaces.ResourceState, global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.ISubscription>> getField6 = (global::System.Func<global::Piraeus.GrainInterfaces.ResourceState, global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.ISubscription>>)global::Orleans.Serialization.SerializationManager.@GetGetter(field6);
        private static readonly global::System.Action<global::Piraeus.GrainInterfaces.ResourceState, global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.ISubscription>> setField6 = (global::System.Action<global::Piraeus.GrainInterfaces.ResourceState, global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.ISubscription>>)global::Orleans.Serialization.SerializationManager.@GetReferenceSetter(field6);
        [global::Orleans.CodeGeneration.CopierMethodAttribute]
        public static global::System.Object DeepCopier(global::System.Object original, global::Orleans.Serialization.ICopyContext context)
        {
            global::Piraeus.GrainInterfaces.ResourceState input = ((global::Piraeus.GrainInterfaces.ResourceState)original);
            global::Piraeus.GrainInterfaces.ResourceState result = new global::Piraeus.GrainInterfaces.ResourceState();
            context.@RecordCopy(original, result);
            result.@ByteCount = input.@ByteCount;
            result.@ErrorCount = input.@ErrorCount;
            result.@ErrorLeases = (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IErrorObserver>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@ErrorLeases, context);
            result.@LastErrorTimestamp = (global::System.Nullable<global::System.DateTime>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@LastErrorTimestamp, context);
            result.@LastMessageTimestamp = (global::System.Nullable<global::System.DateTime>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@LastMessageTimestamp, context);
            result.@LeaseExpiry = (global::System.Collections.Generic.Dictionary<global::System.String, global::System.Tuple<global::System.DateTime, global::System.String>>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@LeaseExpiry, context);
            result.@MessageCount = input.@MessageCount;
            result.@Metadata = (global::Piraeus.Core.Metadata.ResourceMetadata)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@Metadata, context);
            result.@MetricLeases = (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IMetricObserver>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@MetricLeases, context);
            setField6(result, (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.ISubscription>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(getField6(input), context));
            return result;
        }

        [global::Orleans.CodeGeneration.SerializerMethodAttribute]
        public static void Serializer(global::System.Object untypedInput, global::Orleans.Serialization.ISerializationContext context, global::System.Type expected)
        {
            global::Piraeus.GrainInterfaces.ResourceState input = (global::Piraeus.GrainInterfaces.ResourceState)untypedInput;
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@ByteCount, context, typeof (global::System.Int64));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@ErrorCount, context, typeof (global::System.Int64));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@ErrorLeases, context, typeof (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IErrorObserver>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@LastErrorTimestamp, context, typeof (global::System.Nullable<global::System.DateTime>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@LastMessageTimestamp, context, typeof (global::System.Nullable<global::System.DateTime>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@LeaseExpiry, context, typeof (global::System.Collections.Generic.Dictionary<global::System.String, global::System.Tuple<global::System.DateTime, global::System.String>>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@MessageCount, context, typeof (global::System.Int64));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Metadata, context, typeof (global::Piraeus.Core.Metadata.ResourceMetadata));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@MetricLeases, context, typeof (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IMetricObserver>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(getField6(input), context, typeof (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.ISubscription>));
        }

        [global::Orleans.CodeGeneration.DeserializerMethodAttribute]
        public static global::System.Object Deserializer(global::System.Type expected, global::Orleans.Serialization.IDeserializationContext context)
        {
            global::Piraeus.GrainInterfaces.ResourceState result = new global::Piraeus.GrainInterfaces.ResourceState();
            context.@RecordObject(result);
            result.@ByteCount = (global::System.Int64)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Int64), context);
            result.@ErrorCount = (global::System.Int64)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Int64), context);
            result.@ErrorLeases = (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IErrorObserver>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IErrorObserver>), context);
            result.@LastErrorTimestamp = (global::System.Nullable<global::System.DateTime>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Nullable<global::System.DateTime>), context);
            result.@LastMessageTimestamp = (global::System.Nullable<global::System.DateTime>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Nullable<global::System.DateTime>), context);
            result.@LeaseExpiry = (global::System.Collections.Generic.Dictionary<global::System.String, global::System.Tuple<global::System.DateTime, global::System.String>>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Collections.Generic.Dictionary<global::System.String, global::System.Tuple<global::System.DateTime, global::System.String>>), context);
            result.@MessageCount = (global::System.Int64)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Int64), context);
            result.@Metadata = (global::Piraeus.Core.Metadata.ResourceMetadata)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::Piraeus.Core.Metadata.ResourceMetadata), context);
            result.@MetricLeases = (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IMetricObserver>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IMetricObserver>), context);
            setField6(result, (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.ISubscription>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.ISubscription>), context));
            return (global::Piraeus.GrainInterfaces.ResourceState)result;
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.SerializerAttribute(typeof (global::Piraeus.Core.Metadata.ResourceMetadata))]
    internal class OrleansCodeGenPiraeus_Core_Metadata_ResourceMetadataSerializer
    {
        [global::Orleans.CodeGeneration.CopierMethodAttribute]
        public static global::System.Object DeepCopier(global::System.Object original, global::Orleans.Serialization.ICopyContext context)
        {
            global::Piraeus.Core.Metadata.ResourceMetadata input = ((global::Piraeus.Core.Metadata.ResourceMetadata)original);
            global::Piraeus.Core.Metadata.ResourceMetadata result = new global::Piraeus.Core.Metadata.ResourceMetadata();
            context.@RecordCopy(original, result);
            result.@Audit = input.@Audit;
            result.@Description = input.@Description;
            result.@DiscoveryUrl = input.@DiscoveryUrl;
            result.@Enabled = input.@Enabled;
            result.@Expires = (global::System.Nullable<global::System.DateTime>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@Expires, context);
            result.@MaxSubscriptionDuration = (global::System.Nullable<global::System.TimeSpan>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@MaxSubscriptionDuration, context);
            result.@PublishPolicyUriString = input.@PublishPolicyUriString;
            result.@RequireEncryptedChannel = input.@RequireEncryptedChannel;
            result.@ResourceUriString = input.@ResourceUriString;
            result.@SubscribePolicyUriString = input.@SubscribePolicyUriString;
            return result;
        }

        [global::Orleans.CodeGeneration.SerializerMethodAttribute]
        public static void Serializer(global::System.Object untypedInput, global::Orleans.Serialization.ISerializationContext context, global::System.Type expected)
        {
            global::Piraeus.Core.Metadata.ResourceMetadata input = (global::Piraeus.Core.Metadata.ResourceMetadata)untypedInput;
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Audit, context, typeof (global::System.Boolean));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Description, context, typeof (global::System.String));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@DiscoveryUrl, context, typeof (global::System.String));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Enabled, context, typeof (global::System.Boolean));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Expires, context, typeof (global::System.Nullable<global::System.DateTime>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@MaxSubscriptionDuration, context, typeof (global::System.Nullable<global::System.TimeSpan>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@PublishPolicyUriString, context, typeof (global::System.String));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@RequireEncryptedChannel, context, typeof (global::System.Boolean));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@ResourceUriString, context, typeof (global::System.String));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@SubscribePolicyUriString, context, typeof (global::System.String));
        }

        [global::Orleans.CodeGeneration.DeserializerMethodAttribute]
        public static global::System.Object Deserializer(global::System.Type expected, global::Orleans.Serialization.IDeserializationContext context)
        {
            global::Piraeus.Core.Metadata.ResourceMetadata result = new global::Piraeus.Core.Metadata.ResourceMetadata();
            context.@RecordObject(result);
            result.@Audit = (global::System.Boolean)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Boolean), context);
            result.@Description = (global::System.String)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.String), context);
            result.@DiscoveryUrl = (global::System.String)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.String), context);
            result.@Enabled = (global::System.Boolean)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Boolean), context);
            result.@Expires = (global::System.Nullable<global::System.DateTime>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Nullable<global::System.DateTime>), context);
            result.@MaxSubscriptionDuration = (global::System.Nullable<global::System.TimeSpan>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Nullable<global::System.TimeSpan>), context);
            result.@PublishPolicyUriString = (global::System.String)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.String), context);
            result.@RequireEncryptedChannel = (global::System.Boolean)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Boolean), context);
            result.@ResourceUriString = (global::System.String)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.String), context);
            result.@SubscribePolicyUriString = (global::System.String)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.String), context);
            return (global::Piraeus.Core.Metadata.ResourceMetadata)result;
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.SerializerAttribute(typeof (global::Piraeus.GrainInterfaces.ResourceListState))]
    internal class OrleansCodeGenPiraeus_GrainInterfaces_ResourceListStateSerializer
    {
        [global::Orleans.CodeGeneration.CopierMethodAttribute]
        public static global::System.Object DeepCopier(global::System.Object original, global::Orleans.Serialization.ICopyContext context)
        {
            global::Piraeus.GrainInterfaces.ResourceListState input = ((global::Piraeus.GrainInterfaces.ResourceListState)original);
            global::Piraeus.GrainInterfaces.ResourceListState result = new global::Piraeus.GrainInterfaces.ResourceListState();
            context.@RecordCopy(original, result);
            result.@Container = (global::System.Collections.Generic.List<global::System.String>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@Container, context);
            return result;
        }

        [global::Orleans.CodeGeneration.SerializerMethodAttribute]
        public static void Serializer(global::System.Object untypedInput, global::Orleans.Serialization.ISerializationContext context, global::System.Type expected)
        {
            global::Piraeus.GrainInterfaces.ResourceListState input = (global::Piraeus.GrainInterfaces.ResourceListState)untypedInput;
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Container, context, typeof (global::System.Collections.Generic.List<global::System.String>));
        }

        [global::Orleans.CodeGeneration.DeserializerMethodAttribute]
        public static global::System.Object Deserializer(global::System.Type expected, global::Orleans.Serialization.IDeserializationContext context)
        {
            global::Piraeus.GrainInterfaces.ResourceListState result = new global::Piraeus.GrainInterfaces.ResourceListState();
            context.@RecordObject(result);
            result.@Container = (global::System.Collections.Generic.List<global::System.String>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Collections.Generic.List<global::System.String>), context);
            return (global::Piraeus.GrainInterfaces.ResourceListState)result;
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.SerializerAttribute(typeof (global::Piraeus.GrainInterfaces.ServiceIdentityState))]
    internal class OrleansCodeGenPiraeus_GrainInterfaces_ServiceIdentityStateSerializer
    {
        [global::Orleans.CodeGeneration.CopierMethodAttribute]
        public static global::System.Object DeepCopier(global::System.Object original, global::Orleans.Serialization.ICopyContext context)
        {
            global::Piraeus.GrainInterfaces.ServiceIdentityState input = ((global::Piraeus.GrainInterfaces.ServiceIdentityState)original);
            global::Piraeus.GrainInterfaces.ServiceIdentityState result = new global::Piraeus.GrainInterfaces.ServiceIdentityState();
            context.@RecordCopy(original, result);
            result.@Certificate = (global::System.Byte[])global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@Certificate, context);
            result.@Claims = (global::System.Collections.Generic.List<global::System.Collections.Generic.KeyValuePair<global::System.String, global::System.String>>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@Claims, context);
            return result;
        }

        [global::Orleans.CodeGeneration.SerializerMethodAttribute]
        public static void Serializer(global::System.Object untypedInput, global::Orleans.Serialization.ISerializationContext context, global::System.Type expected)
        {
            global::Piraeus.GrainInterfaces.ServiceIdentityState input = (global::Piraeus.GrainInterfaces.ServiceIdentityState)untypedInput;
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Certificate, context, typeof (global::System.Byte[]));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Claims, context, typeof (global::System.Collections.Generic.List<global::System.Collections.Generic.KeyValuePair<global::System.String, global::System.String>>));
        }

        [global::Orleans.CodeGeneration.DeserializerMethodAttribute]
        public static global::System.Object Deserializer(global::System.Type expected, global::Orleans.Serialization.IDeserializationContext context)
        {
            global::Piraeus.GrainInterfaces.ServiceIdentityState result = new global::Piraeus.GrainInterfaces.ServiceIdentityState();
            context.@RecordObject(result);
            result.@Certificate = (global::System.Byte[])global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Byte[]), context);
            result.@Claims = (global::System.Collections.Generic.List<global::System.Collections.Generic.KeyValuePair<global::System.String, global::System.String>>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Collections.Generic.List<global::System.Collections.Generic.KeyValuePair<global::System.String, global::System.String>>), context);
            return (global::Piraeus.GrainInterfaces.ServiceIdentityState)result;
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.SerializerAttribute(typeof (global::Piraeus.GrainInterfaces.SubscriberState))]
    internal class OrleansCodeGenPiraeus_GrainInterfaces_SubscriberStateSerializer
    {
        [global::Orleans.CodeGeneration.CopierMethodAttribute]
        public static global::System.Object DeepCopier(global::System.Object original, global::Orleans.Serialization.ICopyContext context)
        {
            global::Piraeus.GrainInterfaces.SubscriberState input = ((global::Piraeus.GrainInterfaces.SubscriberState)original);
            global::Piraeus.GrainInterfaces.SubscriberState result = new global::Piraeus.GrainInterfaces.SubscriberState();
            context.@RecordCopy(original, result);
            result.@Container = (global::System.Collections.Generic.List<global::System.String>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@Container, context);
            return result;
        }

        [global::Orleans.CodeGeneration.SerializerMethodAttribute]
        public static void Serializer(global::System.Object untypedInput, global::Orleans.Serialization.ISerializationContext context, global::System.Type expected)
        {
            global::Piraeus.GrainInterfaces.SubscriberState input = (global::Piraeus.GrainInterfaces.SubscriberState)untypedInput;
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Container, context, typeof (global::System.Collections.Generic.List<global::System.String>));
        }

        [global::Orleans.CodeGeneration.DeserializerMethodAttribute]
        public static global::System.Object Deserializer(global::System.Type expected, global::Orleans.Serialization.IDeserializationContext context)
        {
            global::Piraeus.GrainInterfaces.SubscriberState result = new global::Piraeus.GrainInterfaces.SubscriberState();
            context.@RecordObject(result);
            result.@Container = (global::System.Collections.Generic.List<global::System.String>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Collections.Generic.List<global::System.String>), context);
            return (global::Piraeus.GrainInterfaces.SubscriberState)result;
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.SerializerAttribute(typeof (global::Piraeus.GrainInterfaces.SubscriptionState))]
    internal class OrleansCodeGenPiraeus_GrainInterfaces_SubscriptionStateSerializer
    {
        [global::Orleans.CodeGeneration.CopierMethodAttribute]
        public static global::System.Object DeepCopier(global::System.Object original, global::Orleans.Serialization.ICopyContext context)
        {
            global::Piraeus.GrainInterfaces.SubscriptionState input = ((global::Piraeus.GrainInterfaces.SubscriptionState)original);
            global::Piraeus.GrainInterfaces.SubscriptionState result = new global::Piraeus.GrainInterfaces.SubscriptionState();
            context.@RecordCopy(original, result);
            result.@ByteCount = input.@ByteCount;
            result.@ErrorCount = input.@ErrorCount;
            result.@ErrorLeases = (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IErrorObserver>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@ErrorLeases, context);
            result.@LastErrorTimestamp = (global::System.Nullable<global::System.DateTime>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@LastErrorTimestamp, context);
            result.@LastMessageTimestamp = (global::System.Nullable<global::System.DateTime>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@LastMessageTimestamp, context);
            result.@LeaseExpiry = (global::System.Collections.Generic.Dictionary<global::System.String, global::System.Tuple<global::System.DateTime, global::System.String>>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@LeaseExpiry, context);
            result.@MessageCount = input.@MessageCount;
            result.@MessageLeases = (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IMessageObserver>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@MessageLeases, context);
            result.@MessageQueue = (global::System.Collections.Generic.Queue<global::Piraeus.Core.Messaging.EventMessage>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@MessageQueue, context);
            result.@Metadata = (global::Piraeus.Core.Metadata.SubscriptionMetadata)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@Metadata, context);
            result.@MetricLeases = (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IMetricObserver>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@MetricLeases, context);
            return result;
        }

        [global::Orleans.CodeGeneration.SerializerMethodAttribute]
        public static void Serializer(global::System.Object untypedInput, global::Orleans.Serialization.ISerializationContext context, global::System.Type expected)
        {
            global::Piraeus.GrainInterfaces.SubscriptionState input = (global::Piraeus.GrainInterfaces.SubscriptionState)untypedInput;
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@ByteCount, context, typeof (global::System.Int64));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@ErrorCount, context, typeof (global::System.Int64));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@ErrorLeases, context, typeof (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IErrorObserver>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@LastErrorTimestamp, context, typeof (global::System.Nullable<global::System.DateTime>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@LastMessageTimestamp, context, typeof (global::System.Nullable<global::System.DateTime>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@LeaseExpiry, context, typeof (global::System.Collections.Generic.Dictionary<global::System.String, global::System.Tuple<global::System.DateTime, global::System.String>>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@MessageCount, context, typeof (global::System.Int64));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@MessageLeases, context, typeof (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IMessageObserver>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@MessageQueue, context, typeof (global::System.Collections.Generic.Queue<global::Piraeus.Core.Messaging.EventMessage>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Metadata, context, typeof (global::Piraeus.Core.Metadata.SubscriptionMetadata));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@MetricLeases, context, typeof (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IMetricObserver>));
        }

        [global::Orleans.CodeGeneration.DeserializerMethodAttribute]
        public static global::System.Object Deserializer(global::System.Type expected, global::Orleans.Serialization.IDeserializationContext context)
        {
            global::Piraeus.GrainInterfaces.SubscriptionState result = new global::Piraeus.GrainInterfaces.SubscriptionState();
            context.@RecordObject(result);
            result.@ByteCount = (global::System.Int64)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Int64), context);
            result.@ErrorCount = (global::System.Int64)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Int64), context);
            result.@ErrorLeases = (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IErrorObserver>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IErrorObserver>), context);
            result.@LastErrorTimestamp = (global::System.Nullable<global::System.DateTime>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Nullable<global::System.DateTime>), context);
            result.@LastMessageTimestamp = (global::System.Nullable<global::System.DateTime>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Nullable<global::System.DateTime>), context);
            result.@LeaseExpiry = (global::System.Collections.Generic.Dictionary<global::System.String, global::System.Tuple<global::System.DateTime, global::System.String>>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Collections.Generic.Dictionary<global::System.String, global::System.Tuple<global::System.DateTime, global::System.String>>), context);
            result.@MessageCount = (global::System.Int64)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Int64), context);
            result.@MessageLeases = (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IMessageObserver>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IMessageObserver>), context);
            result.@MessageQueue = (global::System.Collections.Generic.Queue<global::Piraeus.Core.Messaging.EventMessage>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Collections.Generic.Queue<global::Piraeus.Core.Messaging.EventMessage>), context);
            result.@Metadata = (global::Piraeus.Core.Metadata.SubscriptionMetadata)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::Piraeus.Core.Metadata.SubscriptionMetadata), context);
            result.@MetricLeases = (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IMetricObserver>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Collections.Generic.Dictionary<global::System.String, global::Piraeus.GrainInterfaces.IMetricObserver>), context);
            return (global::Piraeus.GrainInterfaces.SubscriptionState)result;
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.SerializerAttribute(typeof (global::Piraeus.Core.Messaging.EventMessage))]
    internal class OrleansCodeGenPiraeus_Core_Messaging_EventMessageSerializer
    {
        [global::Orleans.CodeGeneration.CopierMethodAttribute]
        public static global::System.Object DeepCopier(global::System.Object original, global::Orleans.Serialization.ICopyContext context)
        {
            global::Piraeus.Core.Messaging.EventMessage input = ((global::Piraeus.Core.Messaging.EventMessage)original);
            global::Piraeus.Core.Messaging.EventMessage result = new global::Piraeus.Core.Messaging.EventMessage();
            context.@RecordCopy(original, result);
            result.@Audit = input.@Audit;
            result.@ContentType = input.@ContentType;
            result.@Message = (global::System.Byte[])global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@Message, context);
            result.@MessageId = input.@MessageId;
            result.@Protocol = input.@Protocol;
            result.@ResourceUri = input.@ResourceUri;
            result.@Timestamp = input.@Timestamp;
            return result;
        }

        [global::Orleans.CodeGeneration.SerializerMethodAttribute]
        public static void Serializer(global::System.Object untypedInput, global::Orleans.Serialization.ISerializationContext context, global::System.Type expected)
        {
            global::Piraeus.Core.Messaging.EventMessage input = (global::Piraeus.Core.Messaging.EventMessage)untypedInput;
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Audit, context, typeof (global::System.Boolean));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@ContentType, context, typeof (global::System.String));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Message, context, typeof (global::System.Byte[]));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@MessageId, context, typeof (global::System.String));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Protocol, context, typeof (global::Piraeus.Core.Messaging.ProtocolType));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@ResourceUri, context, typeof (global::System.String));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Timestamp, context, typeof (global::System.DateTime));
        }

        [global::Orleans.CodeGeneration.DeserializerMethodAttribute]
        public static global::System.Object Deserializer(global::System.Type expected, global::Orleans.Serialization.IDeserializationContext context)
        {
            global::Piraeus.Core.Messaging.EventMessage result = new global::Piraeus.Core.Messaging.EventMessage();
            context.@RecordObject(result);
            result.@Audit = (global::System.Boolean)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Boolean), context);
            result.@ContentType = (global::System.String)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.String), context);
            result.@Message = (global::System.Byte[])global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Byte[]), context);
            result.@MessageId = (global::System.String)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.String), context);
            result.@Protocol = (global::Piraeus.Core.Messaging.ProtocolType)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::Piraeus.Core.Messaging.ProtocolType), context);
            result.@ResourceUri = (global::System.String)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.String), context);
            result.@Timestamp = (global::System.DateTime)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.DateTime), context);
            return (global::Piraeus.Core.Messaging.EventMessage)result;
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.SerializerAttribute(typeof (global::Piraeus.Core.Metadata.SubscriptionMetadata))]
    internal class OrleansCodeGenPiraeus_Core_Metadata_SubscriptionMetadataSerializer
    {
        [global::Orleans.CodeGeneration.CopierMethodAttribute]
        public static global::System.Object DeepCopier(global::System.Object original, global::Orleans.Serialization.ICopyContext context)
        {
            global::Piraeus.Core.Metadata.SubscriptionMetadata input = ((global::Piraeus.Core.Metadata.SubscriptionMetadata)original);
            global::Piraeus.Core.Metadata.SubscriptionMetadata result = new global::Piraeus.Core.Metadata.SubscriptionMetadata();
            context.@RecordCopy(original, result);
            result.@Description = input.@Description;
            result.@DurableMessaging = input.@DurableMessaging;
            result.@Expires = (global::System.Nullable<global::System.DateTime>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@Expires, context);
            result.@Identity = input.@Identity;
            result.@Indexes = (global::System.Collections.Generic.List<global::System.Collections.Generic.KeyValuePair<global::System.String, global::System.String>>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@Indexes, context);
            result.@IsEphemeral = input.@IsEphemeral;
            result.@NotifyAddress = input.@NotifyAddress;
            result.@SpoolRate = (global::System.Nullable<global::System.TimeSpan>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@SpoolRate, context);
            result.@SubscriptionUriString = input.@SubscriptionUriString;
            result.@SymmetricKey = input.@SymmetricKey;
            result.@TTL = (global::System.Nullable<global::System.TimeSpan>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@TTL, context);
            result.@TokenType = (global::System.Nullable<global::Piraeus.Core.Metadata.SecurityTokenType>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@TokenType, context);
            return result;
        }

        [global::Orleans.CodeGeneration.SerializerMethodAttribute]
        public static void Serializer(global::System.Object untypedInput, global::Orleans.Serialization.ISerializationContext context, global::System.Type expected)
        {
            global::Piraeus.Core.Metadata.SubscriptionMetadata input = (global::Piraeus.Core.Metadata.SubscriptionMetadata)untypedInput;
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Description, context, typeof (global::System.String));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@DurableMessaging, context, typeof (global::System.Boolean));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Expires, context, typeof (global::System.Nullable<global::System.DateTime>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Identity, context, typeof (global::System.String));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Indexes, context, typeof (global::System.Collections.Generic.List<global::System.Collections.Generic.KeyValuePair<global::System.String, global::System.String>>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@IsEphemeral, context, typeof (global::System.Boolean));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@NotifyAddress, context, typeof (global::System.String));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@SpoolRate, context, typeof (global::System.Nullable<global::System.TimeSpan>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@SubscriptionUriString, context, typeof (global::System.String));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@SymmetricKey, context, typeof (global::System.String));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@TTL, context, typeof (global::System.Nullable<global::System.TimeSpan>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@TokenType, context, typeof (global::System.Nullable<global::Piraeus.Core.Metadata.SecurityTokenType>));
        }

        [global::Orleans.CodeGeneration.DeserializerMethodAttribute]
        public static global::System.Object Deserializer(global::System.Type expected, global::Orleans.Serialization.IDeserializationContext context)
        {
            global::Piraeus.Core.Metadata.SubscriptionMetadata result = new global::Piraeus.Core.Metadata.SubscriptionMetadata();
            context.@RecordObject(result);
            result.@Description = (global::System.String)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.String), context);
            result.@DurableMessaging = (global::System.Boolean)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Boolean), context);
            result.@Expires = (global::System.Nullable<global::System.DateTime>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Nullable<global::System.DateTime>), context);
            result.@Identity = (global::System.String)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.String), context);
            result.@Indexes = (global::System.Collections.Generic.List<global::System.Collections.Generic.KeyValuePair<global::System.String, global::System.String>>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Collections.Generic.List<global::System.Collections.Generic.KeyValuePair<global::System.String, global::System.String>>), context);
            result.@IsEphemeral = (global::System.Boolean)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Boolean), context);
            result.@NotifyAddress = (global::System.String)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.String), context);
            result.@SpoolRate = (global::System.Nullable<global::System.TimeSpan>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Nullable<global::System.TimeSpan>), context);
            result.@SubscriptionUriString = (global::System.String)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.String), context);
            result.@SymmetricKey = (global::System.String)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.String), context);
            result.@TTL = (global::System.Nullable<global::System.TimeSpan>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Nullable<global::System.TimeSpan>), context);
            result.@TokenType = (global::System.Nullable<global::Piraeus.Core.Metadata.SecurityTokenType>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Nullable<global::Piraeus.Core.Metadata.SecurityTokenType>), context);
            return (global::Piraeus.Core.Metadata.SubscriptionMetadata)result;
        }
    }
}
#pragma warning restore 162
#pragma warning restore 219
#pragma warning restore 414
#pragma warning restore 649
#pragma warning restore 693
#pragma warning restore 1591
#pragma warning restore 1998
#endif
