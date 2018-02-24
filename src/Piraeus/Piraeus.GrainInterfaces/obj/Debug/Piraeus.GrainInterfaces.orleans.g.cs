#if !EXCLUDE_CODEGEN
#pragma warning disable 162
#pragma warning disable 219
#pragma warning disable 414
#pragma warning disable 649
#pragma warning disable 693
#pragma warning disable 1591
#pragma warning disable 1998
[assembly: global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0")]
[assembly: global::Orleans.CodeGeneration.OrleansCodeGenerationTargetAttribute("Piraeus.GrainInterfaces, Version=0.9.2.0, Culture=neutral, PublicKeyToken=null")]
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

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.SerializableAttribute, global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.GrainReferenceAttribute(typeof (global::Piraeus.GrainInterfaces.IAccessControl))]
    internal class OrleansCodeGenAccessControlReference : global::Orleans.Runtime.GrainReference, global::Piraeus.GrainInterfaces.IAccessControl
    {
        protected @OrleansCodeGenAccessControlReference(global::Orleans.Runtime.GrainReference @other): base (@other)
        {
        }

        protected @OrleansCodeGenAccessControlReference(global::System.Runtime.Serialization.SerializationInfo @info, global::System.Runtime.Serialization.StreamingContext @context): base (@info, @context)
        {
        }

        protected override global::System.Int32 InterfaceId
        {
            get
            {
                return 904784176;
            }
        }

        protected override global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }

        public override global::System.String InterfaceName
        {
            get
            {
                return "global::Piraeus.GrainInterfaces.IAccessControl";
            }
        }

        public override global::System.Boolean @IsCompatible(global::System.Int32 @interfaceId)
        {
            return @interfaceId == 904784176 || @interfaceId == -1277021679;
        }

        protected override global::System.String @GetMethodName(global::System.Int32 @interfaceId, global::System.Int32 @methodId)
        {
            switch (@interfaceId)
            {
                case 904784176:
                    switch (@methodId)
                    {
                        case -1016270937:
                            return "UpsertPolicyAsync";
                        case -1649415899:
                            return "ClearAsync";
                        case 894844329:
                            return "GetPolicyAsync";
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + 904784176 + ",methodId=" + @methodId);
                    }

                case -1277021679:
                    switch (@methodId)
                    {
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1277021679 + ",methodId=" + @methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + @interfaceId);
            }
        }

        public global::System.Threading.Tasks.Task @UpsertPolicyAsync(global::Capl.Authorization.AuthorizationPolicy @policy)
        {
            return base.@InvokeMethodAsync<global::System.Object>(-1016270937, new global::System.Object[]{@policy});
        }

        public global::System.Threading.Tasks.Task @ClearAsync()
        {
            return base.@InvokeMethodAsync<global::System.Object>(-1649415899, null);
        }

        public global::System.Threading.Tasks.Task<global::Capl.Authorization.AuthorizationPolicy> @GetPolicyAsync()
        {
            return base.@InvokeMethodAsync<global::Capl.Authorization.AuthorizationPolicy>(894844329, null);
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::Orleans.CodeGeneration.MethodInvokerAttribute(typeof (global::Piraeus.GrainInterfaces.IAccessControl), 904784176), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute]
    internal class OrleansCodeGenAccessControlMethodInvoker : global::Orleans.CodeGeneration.IGrainMethodInvoker
    {
        public global::System.Threading.Tasks.Task<global::System.Object> @Invoke(global::Orleans.Runtime.IAddressable @grain, global::Orleans.CodeGeneration.InvokeMethodRequest @request)
        {
            global::System.Int32 interfaceId = @request.@InterfaceId;
            global::System.Int32 methodId = @request.@MethodId;
            global::System.Object[] arguments = @request.@Arguments;
            if (@grain == null)
                throw new global::System.ArgumentNullException("grain");
            switch (interfaceId)
            {
                case 904784176:
                    switch (methodId)
                    {
                        case -1016270937:
                            return ((global::Piraeus.GrainInterfaces.IAccessControl)@grain).@UpsertPolicyAsync((global::Capl.Authorization.AuthorizationPolicy)arguments[0]).@Box();
                        case -1649415899:
                            return ((global::Piraeus.GrainInterfaces.IAccessControl)@grain).@ClearAsync().@Box();
                        case 894844329:
                            return ((global::Piraeus.GrainInterfaces.IAccessControl)@grain).@GetPolicyAsync().@Box();
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + 904784176 + ",methodId=" + methodId);
                    }

                case -1277021679:
                    switch (methodId)
                    {
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1277021679 + ",methodId=" + methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + interfaceId);
            }
        }

        public global::System.Int32 InterfaceId
        {
            get
            {
                return 904784176;
            }
        }

        public global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.SerializableAttribute, global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.GrainReferenceAttribute(typeof (global::Piraeus.GrainInterfaces.IAuditConfig))]
    internal class OrleansCodeGenAuditConfigReference : global::Orleans.Runtime.GrainReference, global::Piraeus.GrainInterfaces.IAuditConfig
    {
        protected @OrleansCodeGenAuditConfigReference(global::Orleans.Runtime.GrainReference @other): base (@other)
        {
        }

        protected @OrleansCodeGenAuditConfigReference(global::System.Runtime.Serialization.SerializationInfo @info, global::System.Runtime.Serialization.StreamingContext @context): base (@info, @context)
        {
        }

        protected override global::System.Int32 InterfaceId
        {
            get
            {
                return 1164047816;
            }
        }

        protected override global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }

        public override global::System.String InterfaceName
        {
            get
            {
                return "global::Piraeus.GrainInterfaces.IAuditConfig";
            }
        }

        public override global::System.Boolean @IsCompatible(global::System.Int32 @interfaceId)
        {
            return @interfaceId == 1164047816 || @interfaceId == -1277021679;
        }

        protected override global::System.String @GetMethodName(global::System.Int32 @interfaceId, global::System.Int32 @methodId)
        {
            switch (@interfaceId)
            {
                case 1164047816:
                    switch (@methodId)
                    {
                        case 833494346:
                            return "SetAzureStorageConnectionAsync";
                        case -850366061:
                            return "GetConnectionstringAsync";
                        case 1715031117:
                            return "GetTableNameAsync";
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + 1164047816 + ",methodId=" + @methodId);
                    }

                case -1277021679:
                    switch (@methodId)
                    {
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1277021679 + ",methodId=" + @methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + @interfaceId);
            }
        }

        public global::System.Threading.Tasks.Task @SetAzureStorageConnectionAsync(global::System.String @connectionstring, global::System.String @tablename)
        {
            return base.@InvokeMethodAsync<global::System.Object>(833494346, new global::System.Object[]{@connectionstring, @tablename});
        }

        public global::System.Threading.Tasks.Task<global::System.String> @GetConnectionstringAsync()
        {
            return base.@InvokeMethodAsync<global::System.String>(-850366061, null);
        }

        public global::System.Threading.Tasks.Task<global::System.String> @GetTableNameAsync()
        {
            return base.@InvokeMethodAsync<global::System.String>(1715031117, null);
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::Orleans.CodeGeneration.MethodInvokerAttribute(typeof (global::Piraeus.GrainInterfaces.IAuditConfig), 1164047816), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute]
    internal class OrleansCodeGenAuditConfigMethodInvoker : global::Orleans.CodeGeneration.IGrainMethodInvoker
    {
        public global::System.Threading.Tasks.Task<global::System.Object> @Invoke(global::Orleans.Runtime.IAddressable @grain, global::Orleans.CodeGeneration.InvokeMethodRequest @request)
        {
            global::System.Int32 interfaceId = @request.@InterfaceId;
            global::System.Int32 methodId = @request.@MethodId;
            global::System.Object[] arguments = @request.@Arguments;
            if (@grain == null)
                throw new global::System.ArgumentNullException("grain");
            switch (interfaceId)
            {
                case 1164047816:
                    switch (methodId)
                    {
                        case 833494346:
                            return ((global::Piraeus.GrainInterfaces.IAuditConfig)@grain).@SetAzureStorageConnectionAsync((global::System.String)arguments[0], (global::System.String)arguments[1]).@Box();
                        case -850366061:
                            return ((global::Piraeus.GrainInterfaces.IAuditConfig)@grain).@GetConnectionstringAsync().@Box();
                        case 1715031117:
                            return ((global::Piraeus.GrainInterfaces.IAuditConfig)@grain).@GetTableNameAsync().@Box();
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + 1164047816 + ",methodId=" + methodId);
                    }

                case -1277021679:
                    switch (methodId)
                    {
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1277021679 + ",methodId=" + methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + interfaceId);
            }
        }

        public global::System.Int32 InterfaceId
        {
            get
            {
                return 1164047816;
            }
        }

        public global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.SerializableAttribute, global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.GrainReferenceAttribute(typeof (global::Piraeus.GrainInterfaces.IErrorObserver))]
    internal class OrleansCodeGenErrorObserverReference : global::Orleans.Runtime.GrainReference, global::Piraeus.GrainInterfaces.IErrorObserver
    {
        protected @OrleansCodeGenErrorObserverReference(global::Orleans.Runtime.GrainReference @other): base (@other)
        {
        }

        protected @OrleansCodeGenErrorObserverReference(global::System.Runtime.Serialization.SerializationInfo @info, global::System.Runtime.Serialization.StreamingContext @context): base (@info, @context)
        {
        }

        protected override global::System.Int32 InterfaceId
        {
            get
            {
                return -358033921;
            }
        }

        protected override global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }

        public override global::System.String InterfaceName
        {
            get
            {
                return "global::Piraeus.GrainInterfaces.IErrorObserver";
            }
        }

        public override global::System.Boolean @IsCompatible(global::System.Int32 @interfaceId)
        {
            return @interfaceId == -358033921;
        }

        protected override global::System.String @GetMethodName(global::System.Int32 @interfaceId, global::System.Int32 @methodId)
        {
            switch (@interfaceId)
            {
                case -358033921:
                    switch (@methodId)
                    {
                        case -765791451:
                            return "NotifyError";
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -358033921 + ",methodId=" + @methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + @interfaceId);
            }
        }

        public void @NotifyError(global::System.String @grainId, global::System.Exception @error)
        {
            base.@InvokeOneWayMethod(-765791451, new global::System.Object[]{@grainId, @error});
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::Orleans.CodeGeneration.MethodInvokerAttribute(typeof (global::Piraeus.GrainInterfaces.IErrorObserver), -358033921), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute]
    internal class OrleansCodeGenErrorObserverMethodInvoker : global::Orleans.CodeGeneration.IGrainMethodInvoker
    {
        public global::System.Threading.Tasks.Task<global::System.Object> @Invoke(global::Orleans.Runtime.IAddressable @grain, global::Orleans.CodeGeneration.InvokeMethodRequest @request)
        {
            global::System.Int32 interfaceId = @request.@InterfaceId;
            global::System.Int32 methodId = @request.@MethodId;
            global::System.Object[] arguments = @request.@Arguments;
            if (@grain == null)
                throw new global::System.ArgumentNullException("grain");
            switch (interfaceId)
            {
                case -358033921:
                    switch (methodId)
                    {
                        case -765791451:
                            ((global::Piraeus.GrainInterfaces.IErrorObserver)@grain).@NotifyError((global::System.String)arguments[0], (global::System.Exception)arguments[1]);
                            return global::Orleans.Async.TaskUtility.@Completed();
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -358033921 + ",methodId=" + methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + interfaceId);
            }
        }

        public global::System.Int32 InterfaceId
        {
            get
            {
                return -358033921;
            }
        }

        public global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.SerializableAttribute, global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.GrainReferenceAttribute(typeof (global::Piraeus.GrainInterfaces.IMessageObserver))]
    internal class OrleansCodeGenMessageObserverReference : global::Orleans.Runtime.GrainReference, global::Piraeus.GrainInterfaces.IMessageObserver
    {
        protected @OrleansCodeGenMessageObserverReference(global::Orleans.Runtime.GrainReference @other): base (@other)
        {
        }

        protected @OrleansCodeGenMessageObserverReference(global::System.Runtime.Serialization.SerializationInfo @info, global::System.Runtime.Serialization.StreamingContext @context): base (@info, @context)
        {
        }

        protected override global::System.Int32 InterfaceId
        {
            get
            {
                return -1543216846;
            }
        }

        protected override global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }

        public override global::System.String InterfaceName
        {
            get
            {
                return "global::Piraeus.GrainInterfaces.IMessageObserver";
            }
        }

        public override global::System.Boolean @IsCompatible(global::System.Int32 @interfaceId)
        {
            return @interfaceId == -1543216846;
        }

        protected override global::System.String @GetMethodName(global::System.Int32 @interfaceId, global::System.Int32 @methodId)
        {
            switch (@interfaceId)
            {
                case -1543216846:
                    switch (@methodId)
                    {
                        case -1372484765:
                            return "Notify";
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1543216846 + ",methodId=" + @methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + @interfaceId);
            }
        }

        public void @Notify(global::Piraeus.Core.Messaging.EventMessage @message)
        {
            base.@InvokeOneWayMethod(-1372484765, new global::System.Object[]{@message});
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::Orleans.CodeGeneration.MethodInvokerAttribute(typeof (global::Piraeus.GrainInterfaces.IMessageObserver), -1543216846), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute]
    internal class OrleansCodeGenMessageObserverMethodInvoker : global::Orleans.CodeGeneration.IGrainMethodInvoker
    {
        public global::System.Threading.Tasks.Task<global::System.Object> @Invoke(global::Orleans.Runtime.IAddressable @grain, global::Orleans.CodeGeneration.InvokeMethodRequest @request)
        {
            global::System.Int32 interfaceId = @request.@InterfaceId;
            global::System.Int32 methodId = @request.@MethodId;
            global::System.Object[] arguments = @request.@Arguments;
            if (@grain == null)
                throw new global::System.ArgumentNullException("grain");
            switch (interfaceId)
            {
                case -1543216846:
                    switch (methodId)
                    {
                        case -1372484765:
                            ((global::Piraeus.GrainInterfaces.IMessageObserver)@grain).@Notify((global::Piraeus.Core.Messaging.EventMessage)arguments[0]);
                            return global::Orleans.Async.TaskUtility.@Completed();
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1543216846 + ",methodId=" + methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + interfaceId);
            }
        }

        public global::System.Int32 InterfaceId
        {
            get
            {
                return -1543216846;
            }
        }

        public global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.SerializableAttribute, global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.GrainReferenceAttribute(typeof (global::Piraeus.GrainInterfaces.IMetricObserver))]
    internal class OrleansCodeGenMetricObserverReference : global::Orleans.Runtime.GrainReference, global::Piraeus.GrainInterfaces.IMetricObserver
    {
        protected @OrleansCodeGenMetricObserverReference(global::Orleans.Runtime.GrainReference @other): base (@other)
        {
        }

        protected @OrleansCodeGenMetricObserverReference(global::System.Runtime.Serialization.SerializationInfo @info, global::System.Runtime.Serialization.StreamingContext @context): base (@info, @context)
        {
        }

        protected override global::System.Int32 InterfaceId
        {
            get
            {
                return 376256010;
            }
        }

        protected override global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }

        public override global::System.String InterfaceName
        {
            get
            {
                return "global::Piraeus.GrainInterfaces.IMetricObserver";
            }
        }

        public override global::System.Boolean @IsCompatible(global::System.Int32 @interfaceId)
        {
            return @interfaceId == 376256010;
        }

        protected override global::System.String @GetMethodName(global::System.Int32 @interfaceId, global::System.Int32 @methodId)
        {
            switch (@interfaceId)
            {
                case 376256010:
                    switch (@methodId)
                    {
                        case 650212527:
                            return "NotifyMetrics";
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + 376256010 + ",methodId=" + @methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + @interfaceId);
            }
        }

        public void @NotifyMetrics(global::Piraeus.Core.Messaging.CommunicationMetrics @metrics)
        {
            base.@InvokeOneWayMethod(650212527, new global::System.Object[]{@metrics});
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::Orleans.CodeGeneration.MethodInvokerAttribute(typeof (global::Piraeus.GrainInterfaces.IMetricObserver), 376256010), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute]
    internal class OrleansCodeGenMetricObserverMethodInvoker : global::Orleans.CodeGeneration.IGrainMethodInvoker
    {
        public global::System.Threading.Tasks.Task<global::System.Object> @Invoke(global::Orleans.Runtime.IAddressable @grain, global::Orleans.CodeGeneration.InvokeMethodRequest @request)
        {
            global::System.Int32 interfaceId = @request.@InterfaceId;
            global::System.Int32 methodId = @request.@MethodId;
            global::System.Object[] arguments = @request.@Arguments;
            if (@grain == null)
                throw new global::System.ArgumentNullException("grain");
            switch (interfaceId)
            {
                case 376256010:
                    switch (methodId)
                    {
                        case 650212527:
                            ((global::Piraeus.GrainInterfaces.IMetricObserver)@grain).@NotifyMetrics((global::Piraeus.Core.Messaging.CommunicationMetrics)arguments[0]);
                            return global::Orleans.Async.TaskUtility.@Completed();
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + 376256010 + ",methodId=" + methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + interfaceId);
            }
        }

        public global::System.Int32 InterfaceId
        {
            get
            {
                return 376256010;
            }
        }

        public global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.SerializerAttribute(typeof (global::Piraeus.Core.Messaging.CommunicationMetrics))]
    internal class OrleansCodeGenPiraeus_Core_Messaging_CommunicationMetricsSerializer
    {
        [global::Orleans.CodeGeneration.CopierMethodAttribute]
        public static global::System.Object DeepCopier(global::System.Object original, global::Orleans.Serialization.ICopyContext context)
        {
            global::Piraeus.Core.Messaging.CommunicationMetrics input = ((global::Piraeus.Core.Messaging.CommunicationMetrics)original);
            global::Piraeus.Core.Messaging.CommunicationMetrics result = new global::Piraeus.Core.Messaging.CommunicationMetrics();
            context.@RecordCopy(original, result);
            result.@ByteCount = input.@ByteCount;
            result.@ErrorCount = input.@ErrorCount;
            result.@Id = input.@Id;
            result.@LastError = (global::System.Exception)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@LastError, context);
            result.@LastErrorTimestamp = (global::System.Nullable<global::System.DateTime>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@LastErrorTimestamp, context);
            result.@LastMessageTimestamp = (global::System.Nullable<global::System.DateTime>)global::Orleans.Serialization.SerializationManager.@DeepCopyInner(input.@LastMessageTimestamp, context);
            result.@MessageCount = input.@MessageCount;
            return result;
        }

        [global::Orleans.CodeGeneration.SerializerMethodAttribute]
        public static void Serializer(global::System.Object untypedInput, global::Orleans.Serialization.ISerializationContext context, global::System.Type expected)
        {
            global::Piraeus.Core.Messaging.CommunicationMetrics input = (global::Piraeus.Core.Messaging.CommunicationMetrics)untypedInput;
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@ByteCount, context, typeof (global::System.Int64));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@ErrorCount, context, typeof (global::System.Int64));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@Id, context, typeof (global::System.String));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@LastError, context, typeof (global::System.Exception));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@LastErrorTimestamp, context, typeof (global::System.Nullable<global::System.DateTime>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@LastMessageTimestamp, context, typeof (global::System.Nullable<global::System.DateTime>));
            global::Orleans.Serialization.SerializationManager.@SerializeInner(input.@MessageCount, context, typeof (global::System.Int64));
        }

        [global::Orleans.CodeGeneration.DeserializerMethodAttribute]
        public static global::System.Object Deserializer(global::System.Type expected, global::Orleans.Serialization.IDeserializationContext context)
        {
            global::Piraeus.Core.Messaging.CommunicationMetrics result = new global::Piraeus.Core.Messaging.CommunicationMetrics();
            context.@RecordObject(result);
            result.@ByteCount = (global::System.Int64)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Int64), context);
            result.@ErrorCount = (global::System.Int64)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Int64), context);
            result.@Id = (global::System.String)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.String), context);
            result.@LastError = (global::System.Exception)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Exception), context);
            result.@LastErrorTimestamp = (global::System.Nullable<global::System.DateTime>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Nullable<global::System.DateTime>), context);
            result.@LastMessageTimestamp = (global::System.Nullable<global::System.DateTime>)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Nullable<global::System.DateTime>), context);
            result.@MessageCount = (global::System.Int64)global::Orleans.Serialization.SerializationManager.@DeserializeInner(typeof (global::System.Int64), context);
            return (global::Piraeus.Core.Messaging.CommunicationMetrics)result;
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.SerializableAttribute, global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.GrainReferenceAttribute(typeof (global::Piraeus.GrainInterfaces.IResource))]
    internal class OrleansCodeGenResourceReference : global::Orleans.Runtime.GrainReference, global::Piraeus.GrainInterfaces.IResource
    {
        protected @OrleansCodeGenResourceReference(global::Orleans.Runtime.GrainReference @other): base (@other)
        {
        }

        protected @OrleansCodeGenResourceReference(global::System.Runtime.Serialization.SerializationInfo @info, global::System.Runtime.Serialization.StreamingContext @context): base (@info, @context)
        {
        }

        protected override global::System.Int32 InterfaceId
        {
            get
            {
                return -369762324;
            }
        }

        protected override global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }

        public override global::System.String InterfaceName
        {
            get
            {
                return "global::Piraeus.GrainInterfaces.IResource";
            }
        }

        public override global::System.Boolean @IsCompatible(global::System.Int32 @interfaceId)
        {
            return @interfaceId == -369762324 || @interfaceId == -1277021679;
        }

        protected override global::System.String @GetMethodName(global::System.Int32 @interfaceId, global::System.Int32 @methodId)
        {
            switch (@interfaceId)
            {
                case -369762324:
                    switch (@methodId)
                    {
                        case 1120461561:
                            return "UpsertMetadataAsync";
                        case 42119730:
                            return "GetMetricsAsync";
                        case 1255360387:
                            return "GetMetadataAsync";
                        case 1523623891:
                            return "SubscribeAsync";
                        case -100167817:
                            return "UnsubscribeAsync";
                        case -1453851530:
                            return "UnsubscribeAsync";
                        case 1308349933:
                            return "GetSubscriptionListAsync";
                        case -1419223039:
                            return "PublishAsync";
                        case 1725683869:
                            return "PublishAsync";
                        case -1649415899:
                            return "ClearAsync";
                        case 2140683751:
                            return "AddObserverAsync";
                        case -609522259:
                            return "AddObserverAsync";
                        case -1510100919:
                            return "RemoveObserverAsync";
                        case -395996090:
                            return "RenewObserverLeaseAsync";
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -369762324 + ",methodId=" + @methodId);
                    }

                case -1277021679:
                    switch (@methodId)
                    {
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1277021679 + ",methodId=" + @methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + @interfaceId);
            }
        }

        public global::System.Threading.Tasks.Task @UpsertMetadataAsync(global::Piraeus.Core.Metadata.ResourceMetadata @metadata)
        {
            return base.@InvokeMethodAsync<global::System.Object>(1120461561, new global::System.Object[]{@metadata});
        }

        public global::System.Threading.Tasks.Task<global::Piraeus.Core.Messaging.CommunicationMetrics> @GetMetricsAsync()
        {
            return base.@InvokeMethodAsync<global::Piraeus.Core.Messaging.CommunicationMetrics>(42119730, null);
        }

        public global::System.Threading.Tasks.Task<global::Piraeus.Core.Metadata.ResourceMetadata> @GetMetadataAsync()
        {
            return base.@InvokeMethodAsync<global::Piraeus.Core.Metadata.ResourceMetadata>(1255360387, null);
        }

        public global::System.Threading.Tasks.Task @SubscribeAsync(global::Piraeus.GrainInterfaces.ISubscription @subscription)
        {
            return base.@InvokeMethodAsync<global::System.Object>(1523623891, new global::System.Object[]{@subscription is global::Orleans.Grain ? @subscription.@AsReference<global::Piraeus.GrainInterfaces.ISubscription>() : @subscription});
        }

        public global::System.Threading.Tasks.Task @UnsubscribeAsync(global::System.String @subscriptionUriString)
        {
            return base.@InvokeMethodAsync<global::System.Object>(-100167817, new global::System.Object[]{@subscriptionUriString});
        }

        public global::System.Threading.Tasks.Task @UnsubscribeAsync(global::System.String @subscriptionUriString, global::System.String @identity)
        {
            return base.@InvokeMethodAsync<global::System.Object>(-1453851530, new global::System.Object[]{@subscriptionUriString, @identity});
        }

        public global::System.Threading.Tasks.Task<global::System.Collections.Generic.IEnumerable<global::System.String>> @GetSubscriptionListAsync()
        {
            return base.@InvokeMethodAsync<global::System.Collections.Generic.IEnumerable<global::System.String>>(1308349933, null);
        }

        public global::System.Threading.Tasks.Task @PublishAsync(global::Piraeus.Core.Messaging.EventMessage @message)
        {
            return base.@InvokeMethodAsync<global::System.Object>(-1419223039, new global::System.Object[]{@message});
        }

        public global::System.Threading.Tasks.Task @PublishAsync(global::Piraeus.Core.Messaging.EventMessage @message, global::System.Collections.Generic.List<global::System.Collections.Generic.KeyValuePair<global::System.String, global::System.String>> @indexes)
        {
            return base.@InvokeMethodAsync<global::System.Object>(1725683869, new global::System.Object[]{@message, @indexes});
        }

        public global::System.Threading.Tasks.Task @ClearAsync()
        {
            return base.@InvokeMethodAsync<global::System.Object>(-1649415899, null);
        }

        public global::System.Threading.Tasks.Task<global::System.String> @AddObserverAsync(global::System.TimeSpan @lifetime, global::Piraeus.GrainInterfaces.IMetricObserver @observer)
        {
            global::Orleans.CodeGeneration.GrainFactoryBase.@CheckGrainObserverParamInternal(@observer);
            return base.@InvokeMethodAsync<global::System.String>(2140683751, new global::System.Object[]{@lifetime, @observer is global::Orleans.Grain ? @observer.@AsReference<global::Piraeus.GrainInterfaces.IMetricObserver>() : @observer});
        }

        public global::System.Threading.Tasks.Task<global::System.String> @AddObserverAsync(global::System.TimeSpan @lifetime, global::Piraeus.GrainInterfaces.IErrorObserver @observer)
        {
            global::Orleans.CodeGeneration.GrainFactoryBase.@CheckGrainObserverParamInternal(@observer);
            return base.@InvokeMethodAsync<global::System.String>(-609522259, new global::System.Object[]{@lifetime, @observer is global::Orleans.Grain ? @observer.@AsReference<global::Piraeus.GrainInterfaces.IErrorObserver>() : @observer});
        }

        public global::System.Threading.Tasks.Task @RemoveObserverAsync(global::System.String @leaseKey)
        {
            return base.@InvokeMethodAsync<global::System.Object>(-1510100919, new global::System.Object[]{@leaseKey});
        }

        public global::System.Threading.Tasks.Task<global::System.Boolean> @RenewObserverLeaseAsync(global::System.String @leaseKey, global::System.TimeSpan @lifetime)
        {
            return base.@InvokeMethodAsync<global::System.Boolean>(-395996090, new global::System.Object[]{@leaseKey, @lifetime});
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::Orleans.CodeGeneration.MethodInvokerAttribute(typeof (global::Piraeus.GrainInterfaces.IResource), -369762324), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute]
    internal class OrleansCodeGenResourceMethodInvoker : global::Orleans.CodeGeneration.IGrainMethodInvoker
    {
        public global::System.Threading.Tasks.Task<global::System.Object> @Invoke(global::Orleans.Runtime.IAddressable @grain, global::Orleans.CodeGeneration.InvokeMethodRequest @request)
        {
            global::System.Int32 interfaceId = @request.@InterfaceId;
            global::System.Int32 methodId = @request.@MethodId;
            global::System.Object[] arguments = @request.@Arguments;
            if (@grain == null)
                throw new global::System.ArgumentNullException("grain");
            switch (interfaceId)
            {
                case -369762324:
                    switch (methodId)
                    {
                        case 1120461561:
                            return ((global::Piraeus.GrainInterfaces.IResource)@grain).@UpsertMetadataAsync((global::Piraeus.Core.Metadata.ResourceMetadata)arguments[0]).@Box();
                        case 42119730:
                            return ((global::Piraeus.GrainInterfaces.IResource)@grain).@GetMetricsAsync().@Box();
                        case 1255360387:
                            return ((global::Piraeus.GrainInterfaces.IResource)@grain).@GetMetadataAsync().@Box();
                        case 1523623891:
                            return ((global::Piraeus.GrainInterfaces.IResource)@grain).@SubscribeAsync((global::Piraeus.GrainInterfaces.ISubscription)arguments[0]).@Box();
                        case -100167817:
                            return ((global::Piraeus.GrainInterfaces.IResource)@grain).@UnsubscribeAsync((global::System.String)arguments[0]).@Box();
                        case -1453851530:
                            return ((global::Piraeus.GrainInterfaces.IResource)@grain).@UnsubscribeAsync((global::System.String)arguments[0], (global::System.String)arguments[1]).@Box();
                        case 1308349933:
                            return ((global::Piraeus.GrainInterfaces.IResource)@grain).@GetSubscriptionListAsync().@Box();
                        case -1419223039:
                            return ((global::Piraeus.GrainInterfaces.IResource)@grain).@PublishAsync((global::Piraeus.Core.Messaging.EventMessage)arguments[0]).@Box();
                        case 1725683869:
                            return ((global::Piraeus.GrainInterfaces.IResource)@grain).@PublishAsync((global::Piraeus.Core.Messaging.EventMessage)arguments[0], (global::System.Collections.Generic.List<global::System.Collections.Generic.KeyValuePair<global::System.String, global::System.String>>)arguments[1]).@Box();
                        case -1649415899:
                            return ((global::Piraeus.GrainInterfaces.IResource)@grain).@ClearAsync().@Box();
                        case 2140683751:
                            return ((global::Piraeus.GrainInterfaces.IResource)@grain).@AddObserverAsync((global::System.TimeSpan)arguments[0], (global::Piraeus.GrainInterfaces.IMetricObserver)arguments[1]).@Box();
                        case -609522259:
                            return ((global::Piraeus.GrainInterfaces.IResource)@grain).@AddObserverAsync((global::System.TimeSpan)arguments[0], (global::Piraeus.GrainInterfaces.IErrorObserver)arguments[1]).@Box();
                        case -1510100919:
                            return ((global::Piraeus.GrainInterfaces.IResource)@grain).@RemoveObserverAsync((global::System.String)arguments[0]).@Box();
                        case -395996090:
                            return ((global::Piraeus.GrainInterfaces.IResource)@grain).@RenewObserverLeaseAsync((global::System.String)arguments[0], (global::System.TimeSpan)arguments[1]).@Box();
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -369762324 + ",methodId=" + methodId);
                    }

                case -1277021679:
                    switch (methodId)
                    {
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1277021679 + ",methodId=" + methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + interfaceId);
            }
        }

        public global::System.Int32 InterfaceId
        {
            get
            {
                return -369762324;
            }
        }

        public global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.SerializableAttribute, global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.GrainReferenceAttribute(typeof (global::Piraeus.GrainInterfaces.IResourceList))]
    internal class OrleansCodeGenResourceListReference : global::Orleans.Runtime.GrainReference, global::Piraeus.GrainInterfaces.IResourceList
    {
        protected @OrleansCodeGenResourceListReference(global::Orleans.Runtime.GrainReference @other): base (@other)
        {
        }

        protected @OrleansCodeGenResourceListReference(global::System.Runtime.Serialization.SerializationInfo @info, global::System.Runtime.Serialization.StreamingContext @context): base (@info, @context)
        {
        }

        protected override global::System.Int32 InterfaceId
        {
            get
            {
                return -1548578914;
            }
        }

        protected override global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }

        public override global::System.String InterfaceName
        {
            get
            {
                return "global::Piraeus.GrainInterfaces.IResourceList";
            }
        }

        public override global::System.Boolean @IsCompatible(global::System.Int32 @interfaceId)
        {
            return @interfaceId == -1548578914 || @interfaceId == -1277021679;
        }

        protected override global::System.String @GetMethodName(global::System.Int32 @interfaceId, global::System.Int32 @methodId)
        {
            switch (@interfaceId)
            {
                case -1548578914:
                    switch (@methodId)
                    {
                        case -812122852:
                            return "AddAsync";
                        case 1024496374:
                            return "RemoveAsync";
                        case -1649415899:
                            return "ClearAsync";
                        case -1419684836:
                            return "GetListAsync";
                        case 940498620:
                            return "Contains";
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1548578914 + ",methodId=" + @methodId);
                    }

                case -1277021679:
                    switch (@methodId)
                    {
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1277021679 + ",methodId=" + @methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + @interfaceId);
            }
        }

        public global::System.Threading.Tasks.Task @AddAsync(global::System.String @resourceUriString)
        {
            return base.@InvokeMethodAsync<global::System.Object>(-812122852, new global::System.Object[]{@resourceUriString});
        }

        public global::System.Threading.Tasks.Task @RemoveAsync(global::System.String @resourceUriString)
        {
            return base.@InvokeMethodAsync<global::System.Object>(1024496374, new global::System.Object[]{@resourceUriString});
        }

        public global::System.Threading.Tasks.Task @ClearAsync()
        {
            return base.@InvokeMethodAsync<global::System.Object>(-1649415899, null);
        }

        public global::System.Threading.Tasks.Task<global::System.Collections.Generic.IEnumerable<global::System.String>> @GetListAsync()
        {
            return base.@InvokeMethodAsync<global::System.Collections.Generic.IEnumerable<global::System.String>>(-1419684836, null);
        }

        public global::System.Threading.Tasks.Task<global::System.Boolean> @Contains(global::System.String @resourceUriString)
        {
            return base.@InvokeMethodAsync<global::System.Boolean>(940498620, new global::System.Object[]{@resourceUriString});
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::Orleans.CodeGeneration.MethodInvokerAttribute(typeof (global::Piraeus.GrainInterfaces.IResourceList), -1548578914), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute]
    internal class OrleansCodeGenResourceListMethodInvoker : global::Orleans.CodeGeneration.IGrainMethodInvoker
    {
        public global::System.Threading.Tasks.Task<global::System.Object> @Invoke(global::Orleans.Runtime.IAddressable @grain, global::Orleans.CodeGeneration.InvokeMethodRequest @request)
        {
            global::System.Int32 interfaceId = @request.@InterfaceId;
            global::System.Int32 methodId = @request.@MethodId;
            global::System.Object[] arguments = @request.@Arguments;
            if (@grain == null)
                throw new global::System.ArgumentNullException("grain");
            switch (interfaceId)
            {
                case -1548578914:
                    switch (methodId)
                    {
                        case -812122852:
                            return ((global::Piraeus.GrainInterfaces.IResourceList)@grain).@AddAsync((global::System.String)arguments[0]).@Box();
                        case 1024496374:
                            return ((global::Piraeus.GrainInterfaces.IResourceList)@grain).@RemoveAsync((global::System.String)arguments[0]).@Box();
                        case -1649415899:
                            return ((global::Piraeus.GrainInterfaces.IResourceList)@grain).@ClearAsync().@Box();
                        case -1419684836:
                            return ((global::Piraeus.GrainInterfaces.IResourceList)@grain).@GetListAsync().@Box();
                        case 940498620:
                            return ((global::Piraeus.GrainInterfaces.IResourceList)@grain).@Contains((global::System.String)arguments[0]).@Box();
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1548578914 + ",methodId=" + methodId);
                    }

                case -1277021679:
                    switch (methodId)
                    {
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1277021679 + ",methodId=" + methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + interfaceId);
            }
        }

        public global::System.Int32 InterfaceId
        {
            get
            {
                return -1548578914;
            }
        }

        public global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.SerializableAttribute, global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.GrainReferenceAttribute(typeof (global::Piraeus.GrainInterfaces.IServiceIdentity))]
    internal class OrleansCodeGenServiceIdentityReference : global::Orleans.Runtime.GrainReference, global::Piraeus.GrainInterfaces.IServiceIdentity
    {
        protected @OrleansCodeGenServiceIdentityReference(global::Orleans.Runtime.GrainReference @other): base (@other)
        {
        }

        protected @OrleansCodeGenServiceIdentityReference(global::System.Runtime.Serialization.SerializationInfo @info, global::System.Runtime.Serialization.StreamingContext @context): base (@info, @context)
        {
        }

        protected override global::System.Int32 InterfaceId
        {
            get
            {
                return 2087181860;
            }
        }

        protected override global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }

        public override global::System.String InterfaceName
        {
            get
            {
                return "global::Piraeus.GrainInterfaces.IServiceIdentity";
            }
        }

        public override global::System.Boolean @IsCompatible(global::System.Int32 @interfaceId)
        {
            return @interfaceId == 2087181860 || @interfaceId == -1277021679;
        }

        protected override global::System.String @GetMethodName(global::System.Int32 @interfaceId, global::System.Int32 @methodId)
        {
            switch (@interfaceId)
            {
                case 2087181860:
                    switch (@methodId)
                    {
                        case 334151400:
                            return "GetCertificateAsync";
                        case 1994790479:
                            return "GetClaimsAsync";
                        case -470715099:
                            return "AddCertificateAsync";
                        case 1369459598:
                            return "AddClaimsAsync";
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + 2087181860 + ",methodId=" + @methodId);
                    }

                case -1277021679:
                    switch (@methodId)
                    {
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1277021679 + ",methodId=" + @methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + @interfaceId);
            }
        }

        public global::System.Threading.Tasks.Task<global::System.Byte[]> @GetCertificateAsync()
        {
            return base.@InvokeMethodAsync<global::System.Byte[]>(334151400, null);
        }

        public global::System.Threading.Tasks.Task<global::System.Collections.Generic.List<global::System.Collections.Generic.KeyValuePair<global::System.String, global::System.String>>> @GetClaimsAsync()
        {
            return base.@InvokeMethodAsync<global::System.Collections.Generic.List<global::System.Collections.Generic.KeyValuePair<global::System.String, global::System.String>>>(1994790479, null);
        }

        public global::System.Threading.Tasks.Task @AddCertificateAsync(global::System.Byte[] @certificate)
        {
            return base.@InvokeMethodAsync<global::System.Object>(-470715099, new global::System.Object[]{@certificate});
        }

        public global::System.Threading.Tasks.Task @AddClaimsAsync(global::System.Collections.Generic.List<global::System.Collections.Generic.KeyValuePair<global::System.String, global::System.String>> @claims)
        {
            return base.@InvokeMethodAsync<global::System.Object>(1369459598, new global::System.Object[]{@claims});
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::Orleans.CodeGeneration.MethodInvokerAttribute(typeof (global::Piraeus.GrainInterfaces.IServiceIdentity), 2087181860), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute]
    internal class OrleansCodeGenServiceIdentityMethodInvoker : global::Orleans.CodeGeneration.IGrainMethodInvoker
    {
        public global::System.Threading.Tasks.Task<global::System.Object> @Invoke(global::Orleans.Runtime.IAddressable @grain, global::Orleans.CodeGeneration.InvokeMethodRequest @request)
        {
            global::System.Int32 interfaceId = @request.@InterfaceId;
            global::System.Int32 methodId = @request.@MethodId;
            global::System.Object[] arguments = @request.@Arguments;
            if (@grain == null)
                throw new global::System.ArgumentNullException("grain");
            switch (interfaceId)
            {
                case 2087181860:
                    switch (methodId)
                    {
                        case 334151400:
                            return ((global::Piraeus.GrainInterfaces.IServiceIdentity)@grain).@GetCertificateAsync().@Box();
                        case 1994790479:
                            return ((global::Piraeus.GrainInterfaces.IServiceIdentity)@grain).@GetClaimsAsync().@Box();
                        case -470715099:
                            return ((global::Piraeus.GrainInterfaces.IServiceIdentity)@grain).@AddCertificateAsync((global::System.Byte[])arguments[0]).@Box();
                        case 1369459598:
                            return ((global::Piraeus.GrainInterfaces.IServiceIdentity)@grain).@AddClaimsAsync((global::System.Collections.Generic.List<global::System.Collections.Generic.KeyValuePair<global::System.String, global::System.String>>)arguments[0]).@Box();
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + 2087181860 + ",methodId=" + methodId);
                    }

                case -1277021679:
                    switch (methodId)
                    {
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1277021679 + ",methodId=" + methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + interfaceId);
            }
        }

        public global::System.Int32 InterfaceId
        {
            get
            {
                return 2087181860;
            }
        }

        public global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.SerializableAttribute, global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.GrainReferenceAttribute(typeof (global::Piraeus.GrainInterfaces.ISubscriber))]
    internal class OrleansCodeGenSubscriberReference : global::Orleans.Runtime.GrainReference, global::Piraeus.GrainInterfaces.ISubscriber
    {
        protected @OrleansCodeGenSubscriberReference(global::Orleans.Runtime.GrainReference @other): base (@other)
        {
        }

        protected @OrleansCodeGenSubscriberReference(global::System.Runtime.Serialization.SerializationInfo @info, global::System.Runtime.Serialization.StreamingContext @context): base (@info, @context)
        {
        }

        protected override global::System.Int32 InterfaceId
        {
            get
            {
                return 678607351;
            }
        }

        protected override global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }

        public override global::System.String InterfaceName
        {
            get
            {
                return "global::Piraeus.GrainInterfaces.ISubscriber";
            }
        }

        public override global::System.Boolean @IsCompatible(global::System.Int32 @interfaceId)
        {
            return @interfaceId == 678607351 || @interfaceId == -1277021679;
        }

        protected override global::System.String @GetMethodName(global::System.Int32 @interfaceId, global::System.Int32 @methodId)
        {
            switch (@interfaceId)
            {
                case 678607351:
                    switch (@methodId)
                    {
                        case -923342660:
                            return "AddSubscriptionAsync";
                        case -1865871640:
                            return "RemoveSubscriptionAsync";
                        case 1151401881:
                            return "GetSubscriptionsAsync";
                        case -1649415899:
                            return "ClearAsync";
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + 678607351 + ",methodId=" + @methodId);
                    }

                case -1277021679:
                    switch (@methodId)
                    {
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1277021679 + ",methodId=" + @methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + @interfaceId);
            }
        }

        public global::System.Threading.Tasks.Task @AddSubscriptionAsync(global::System.String @subscriptionUriString)
        {
            return base.@InvokeMethodAsync<global::System.Object>(-923342660, new global::System.Object[]{@subscriptionUriString});
        }

        public global::System.Threading.Tasks.Task @RemoveSubscriptionAsync(global::System.String @subscriptionUriString)
        {
            return base.@InvokeMethodAsync<global::System.Object>(-1865871640, new global::System.Object[]{@subscriptionUriString});
        }

        public global::System.Threading.Tasks.Task<global::System.Collections.Generic.IEnumerable<global::System.String>> @GetSubscriptionsAsync()
        {
            return base.@InvokeMethodAsync<global::System.Collections.Generic.IEnumerable<global::System.String>>(1151401881, null);
        }

        public global::System.Threading.Tasks.Task @ClearAsync()
        {
            return base.@InvokeMethodAsync<global::System.Object>(-1649415899, null);
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::Orleans.CodeGeneration.MethodInvokerAttribute(typeof (global::Piraeus.GrainInterfaces.ISubscriber), 678607351), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute]
    internal class OrleansCodeGenSubscriberMethodInvoker : global::Orleans.CodeGeneration.IGrainMethodInvoker
    {
        public global::System.Threading.Tasks.Task<global::System.Object> @Invoke(global::Orleans.Runtime.IAddressable @grain, global::Orleans.CodeGeneration.InvokeMethodRequest @request)
        {
            global::System.Int32 interfaceId = @request.@InterfaceId;
            global::System.Int32 methodId = @request.@MethodId;
            global::System.Object[] arguments = @request.@Arguments;
            if (@grain == null)
                throw new global::System.ArgumentNullException("grain");
            switch (interfaceId)
            {
                case 678607351:
                    switch (methodId)
                    {
                        case -923342660:
                            return ((global::Piraeus.GrainInterfaces.ISubscriber)@grain).@AddSubscriptionAsync((global::System.String)arguments[0]).@Box();
                        case -1865871640:
                            return ((global::Piraeus.GrainInterfaces.ISubscriber)@grain).@RemoveSubscriptionAsync((global::System.String)arguments[0]).@Box();
                        case 1151401881:
                            return ((global::Piraeus.GrainInterfaces.ISubscriber)@grain).@GetSubscriptionsAsync().@Box();
                        case -1649415899:
                            return ((global::Piraeus.GrainInterfaces.ISubscriber)@grain).@ClearAsync().@Box();
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + 678607351 + ",methodId=" + methodId);
                    }

                case -1277021679:
                    switch (methodId)
                    {
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1277021679 + ",methodId=" + methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + interfaceId);
            }
        }

        public global::System.Int32 InterfaceId
        {
            get
            {
                return 678607351;
            }
        }

        public global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::System.SerializableAttribute, global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute, global::Orleans.CodeGeneration.GrainReferenceAttribute(typeof (global::Piraeus.GrainInterfaces.ISubscription))]
    internal class OrleansCodeGenSubscriptionReference : global::Orleans.Runtime.GrainReference, global::Piraeus.GrainInterfaces.ISubscription
    {
        protected @OrleansCodeGenSubscriptionReference(global::Orleans.Runtime.GrainReference @other): base (@other)
        {
        }

        protected @OrleansCodeGenSubscriptionReference(global::System.Runtime.Serialization.SerializationInfo @info, global::System.Runtime.Serialization.StreamingContext @context): base (@info, @context)
        {
        }

        protected override global::System.Int32 InterfaceId
        {
            get
            {
                return 1486087461;
            }
        }

        protected override global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
        }

        public override global::System.String InterfaceName
        {
            get
            {
                return "global::Piraeus.GrainInterfaces.ISubscription";
            }
        }

        public override global::System.Boolean @IsCompatible(global::System.Int32 @interfaceId)
        {
            return @interfaceId == 1486087461 || @interfaceId == -1277021679;
        }

        protected override global::System.String @GetMethodName(global::System.Int32 @interfaceId, global::System.Int32 @methodId)
        {
            switch (@interfaceId)
            {
                case 1486087461:
                    switch (@methodId)
                    {
                        case -767118177:
                            return "GetIdAsync";
                        case 233903696:
                            return "UpsertMetadataAsync";
                        case 42119730:
                            return "GetMetricsAsync";
                        case 1255360387:
                            return "GetMetadataAsync";
                        case 1347980661:
                            return "AddObserverAsync";
                        case 2140683751:
                            return "AddObserverAsync";
                        case -609522259:
                            return "AddObserverAsync";
                        case -1510100919:
                            return "RemoveObserverAsync";
                        case -395996090:
                            return "RenewObserverLeaseAsync";
                        case 833284507:
                            return "NotifyAsync";
                        case 934898313:
                            return "NotifyAsync";
                        case -1649415899:
                            return "ClearAsync";
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + 1486087461 + ",methodId=" + @methodId);
                    }

                case -1277021679:
                    switch (@methodId)
                    {
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1277021679 + ",methodId=" + @methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + @interfaceId);
            }
        }

        public global::System.Threading.Tasks.Task<global::System.String> @GetIdAsync()
        {
            return base.@InvokeMethodAsync<global::System.String>(-767118177, null);
        }

        public global::System.Threading.Tasks.Task @UpsertMetadataAsync(global::Piraeus.Core.Metadata.SubscriptionMetadata @metadata)
        {
            return base.@InvokeMethodAsync<global::System.Object>(233903696, new global::System.Object[]{@metadata});
        }

        public global::System.Threading.Tasks.Task<global::Piraeus.Core.Messaging.CommunicationMetrics> @GetMetricsAsync()
        {
            return base.@InvokeMethodAsync<global::Piraeus.Core.Messaging.CommunicationMetrics>(42119730, null);
        }

        public global::System.Threading.Tasks.Task<global::Piraeus.Core.Metadata.SubscriptionMetadata> @GetMetadataAsync()
        {
            return base.@InvokeMethodAsync<global::Piraeus.Core.Metadata.SubscriptionMetadata>(1255360387, null);
        }

        public global::System.Threading.Tasks.Task<global::System.String> @AddObserverAsync(global::System.TimeSpan @lifetime, global::Piraeus.GrainInterfaces.IMessageObserver @observer)
        {
            global::Orleans.CodeGeneration.GrainFactoryBase.@CheckGrainObserverParamInternal(@observer);
            return base.@InvokeMethodAsync<global::System.String>(1347980661, new global::System.Object[]{@lifetime, @observer is global::Orleans.Grain ? @observer.@AsReference<global::Piraeus.GrainInterfaces.IMessageObserver>() : @observer});
        }

        public global::System.Threading.Tasks.Task<global::System.String> @AddObserverAsync(global::System.TimeSpan @lifetime, global::Piraeus.GrainInterfaces.IMetricObserver @observer)
        {
            global::Orleans.CodeGeneration.GrainFactoryBase.@CheckGrainObserverParamInternal(@observer);
            return base.@InvokeMethodAsync<global::System.String>(2140683751, new global::System.Object[]{@lifetime, @observer is global::Orleans.Grain ? @observer.@AsReference<global::Piraeus.GrainInterfaces.IMetricObserver>() : @observer});
        }

        public global::System.Threading.Tasks.Task<global::System.String> @AddObserverAsync(global::System.TimeSpan @lifetime, global::Piraeus.GrainInterfaces.IErrorObserver @observer)
        {
            global::Orleans.CodeGeneration.GrainFactoryBase.@CheckGrainObserverParamInternal(@observer);
            return base.@InvokeMethodAsync<global::System.String>(-609522259, new global::System.Object[]{@lifetime, @observer is global::Orleans.Grain ? @observer.@AsReference<global::Piraeus.GrainInterfaces.IErrorObserver>() : @observer});
        }

        public global::System.Threading.Tasks.Task @RemoveObserverAsync(global::System.String @leaseKey)
        {
            return base.@InvokeMethodAsync<global::System.Object>(-1510100919, new global::System.Object[]{@leaseKey});
        }

        public global::System.Threading.Tasks.Task<global::System.Boolean> @RenewObserverLeaseAsync(global::System.String @leaseKey, global::System.TimeSpan @lifetime)
        {
            return base.@InvokeMethodAsync<global::System.Boolean>(-395996090, new global::System.Object[]{@leaseKey, @lifetime});
        }

        public global::System.Threading.Tasks.Task @NotifyAsync(global::Piraeus.Core.Messaging.EventMessage @message)
        {
            return base.@InvokeMethodAsync<global::System.Object>(833284507, new global::System.Object[]{@message});
        }

        public global::System.Threading.Tasks.Task @NotifyAsync(global::Piraeus.Core.Messaging.EventMessage @message, global::System.Collections.Generic.List<global::System.Collections.Generic.KeyValuePair<global::System.String, global::System.String>> @indexes)
        {
            return base.@InvokeMethodAsync<global::System.Object>(934898313, new global::System.Object[]{@message, @indexes});
        }

        public global::System.Threading.Tasks.Task @ClearAsync()
        {
            return base.@InvokeMethodAsync<global::System.Object>(-1649415899, null);
        }
    }

    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.5.2.0"), global::Orleans.CodeGeneration.MethodInvokerAttribute(typeof (global::Piraeus.GrainInterfaces.ISubscription), 1486087461), global::System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute]
    internal class OrleansCodeGenSubscriptionMethodInvoker : global::Orleans.CodeGeneration.IGrainMethodInvoker
    {
        public global::System.Threading.Tasks.Task<global::System.Object> @Invoke(global::Orleans.Runtime.IAddressable @grain, global::Orleans.CodeGeneration.InvokeMethodRequest @request)
        {
            global::System.Int32 interfaceId = @request.@InterfaceId;
            global::System.Int32 methodId = @request.@MethodId;
            global::System.Object[] arguments = @request.@Arguments;
            if (@grain == null)
                throw new global::System.ArgumentNullException("grain");
            switch (interfaceId)
            {
                case 1486087461:
                    switch (methodId)
                    {
                        case -767118177:
                            return ((global::Piraeus.GrainInterfaces.ISubscription)@grain).@GetIdAsync().@Box();
                        case 233903696:
                            return ((global::Piraeus.GrainInterfaces.ISubscription)@grain).@UpsertMetadataAsync((global::Piraeus.Core.Metadata.SubscriptionMetadata)arguments[0]).@Box();
                        case 42119730:
                            return ((global::Piraeus.GrainInterfaces.ISubscription)@grain).@GetMetricsAsync().@Box();
                        case 1255360387:
                            return ((global::Piraeus.GrainInterfaces.ISubscription)@grain).@GetMetadataAsync().@Box();
                        case 1347980661:
                            return ((global::Piraeus.GrainInterfaces.ISubscription)@grain).@AddObserverAsync((global::System.TimeSpan)arguments[0], (global::Piraeus.GrainInterfaces.IMessageObserver)arguments[1]).@Box();
                        case 2140683751:
                            return ((global::Piraeus.GrainInterfaces.ISubscription)@grain).@AddObserverAsync((global::System.TimeSpan)arguments[0], (global::Piraeus.GrainInterfaces.IMetricObserver)arguments[1]).@Box();
                        case -609522259:
                            return ((global::Piraeus.GrainInterfaces.ISubscription)@grain).@AddObserverAsync((global::System.TimeSpan)arguments[0], (global::Piraeus.GrainInterfaces.IErrorObserver)arguments[1]).@Box();
                        case -1510100919:
                            return ((global::Piraeus.GrainInterfaces.ISubscription)@grain).@RemoveObserverAsync((global::System.String)arguments[0]).@Box();
                        case -395996090:
                            return ((global::Piraeus.GrainInterfaces.ISubscription)@grain).@RenewObserverLeaseAsync((global::System.String)arguments[0], (global::System.TimeSpan)arguments[1]).@Box();
                        case 833284507:
                            return ((global::Piraeus.GrainInterfaces.ISubscription)@grain).@NotifyAsync((global::Piraeus.Core.Messaging.EventMessage)arguments[0]).@Box();
                        case 934898313:
                            return ((global::Piraeus.GrainInterfaces.ISubscription)@grain).@NotifyAsync((global::Piraeus.Core.Messaging.EventMessage)arguments[0], (global::System.Collections.Generic.List<global::System.Collections.Generic.KeyValuePair<global::System.String, global::System.String>>)arguments[1]).@Box();
                        case -1649415899:
                            return ((global::Piraeus.GrainInterfaces.ISubscription)@grain).@ClearAsync().@Box();
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + 1486087461 + ",methodId=" + methodId);
                    }

                case -1277021679:
                    switch (methodId)
                    {
                        default:
                            throw new global::System.NotImplementedException("interfaceId=" + -1277021679 + ",methodId=" + methodId);
                    }

                default:
                    throw new global::System.NotImplementedException("interfaceId=" + interfaceId);
            }
        }

        public global::System.Int32 InterfaceId
        {
            get
            {
                return 1486087461;
            }
        }

        public global::System.UInt16 InterfaceVersion
        {
            get
            {
                return 1;
            }
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
