// -----------------------------------------------------------------------------------------
// <copyright file="AZSCloudAppendBlob.h" company="Microsoft">
//    Copyright 2015 Microsoft Corporation
//
//    Licensed under the MIT License;
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//      http://spdx.org/licenses/MIT
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// </copyright>
// -----------------------------------------------------------------------------------------

#import <CommonCrypto/CommonDigest.h>
#import "AZSCloudAppendBlob.h"
#import "AZSStorageUri.h"
#import "AZSBlobProperties.h"
#import "AZSOperationContext.h"
#import "AZSBlobRequestOptions.h"
#import "AZSStorageCommand.h"
#import "AZSBlobRequestFactory.h"
#import "AZSCloudBlobClient.h"
#import "AZSResponseParser.h"
#import "AZSExecutor.h"
#import "AZSBlobResponseParser.h"

@implementation AZSCloudAppendBlob

- (instancetype)initWithUrl:(NSURL *)blobAbsoluteUrl
{
    return [self initWithUrl:blobAbsoluteUrl credentials:nil snapshotTime:nil];
}
- (instancetype)initWithUrl:(NSURL *)blobAbsoluteUrl credentials:(AZSStorageCredentials *)credentials snapshotTime:(NSString *)snapshotTime
{
    return [self initWithStorageUri:[[AZSStorageUri alloc] initWithPrimaryUri:blobAbsoluteUrl] credentials:credentials snapshotTime:snapshotTime];
}
- (instancetype)initWithStorageUri:(AZSStorageUri *)blobAbsoluteUri
{
    return [self initWithStorageUri:blobAbsoluteUri credentials:nil snapshotTime:nil];
}
- (instancetype)initWithStorageUri:(AZSStorageUri *)blobAbsoluteUri credentials:(AZSStorageCredentials *)credentials snapshotTime:(NSString *)snapshotTime
{
    self = [super initWithStorageUri:blobAbsoluteUri credentials:credentials snapshotTime:snapshotTime];
    if (self)
    {
        self.properties.blobType = AZSBlobTypeAppendBlob;
    }
    
    return self;
}

-(instancetype)initWithContainer:(AZSCloudBlobContainer *)blobContainer name:(NSString *)blobName
{
    return [self initWithContainer:blobContainer name:blobName snapshotTime:nil];
}

-(instancetype)initWithContainer:(AZSCloudBlobContainer *)blobContainer name:(NSString *)blobName snapshotTime:(NSString *)snapshotTime
{
    self =[super initWithContainer:blobContainer name:blobName snapshotTime:snapshotTime];
    if (self)
    {
        self.properties.blobType = AZSBlobTypeAppendBlob;
    }
    
    return self;
}

-(void)createWithCompletionHandler:(void (^)(NSError * __AZSNullable))completionHandler
{
    [self createWithAccessCondition:nil requestOptions:nil operationContext:nil completionHandler:completionHandler];
}

-(void)createWithAccessCondition:(AZSNullable AZSAccessCondition *)accessCondition requestOptions:(AZSNullable AZSBlobRequestOptions *)requestOptions operationContext:(AZSNullable AZSOperationContext *)operationContext completionHandler:(void (^)(NSError * __AZSNullable))completionHandler
{
    if (!operationContext)
    {
        operationContext = [[AZSOperationContext alloc] init];
    }
    AZSBlobRequestOptions *modifiedOptions = [[AZSBlobRequestOptions copyOptions:requestOptions] applyDefaultsFromOptions:self.client.defaultRequestOptions];
    AZSStorageCommand * command = [[AZSStorageCommand alloc] initWithStorageCredentials:self.client.credentials storageUri:self.storageUri operationContext:operationContext];
    
    [command setBuildRequest:^ NSMutableURLRequest * (NSURLComponents *urlComponents, NSTimeInterval timeout, AZSOperationContext *operationContext)
     {
         return [AZSBlobRequestFactory createAppendBlobWithBlobProperties:self.properties cloudMetadata:self.metadata accessCondition:accessCondition urlComponents:urlComponents timeout:timeout operationContext:operationContext];
     }];
    
    [command setAuthenticationHandler:self.client.authenticationHandler];
    
    [command setPreProcessResponse:^id(NSHTTPURLResponse * urlResponse, AZSRequestResult * requestResult, AZSOperationContext * operationContext) {
        NSError *error = [AZSResponseParser preprocessResponseWithResponse:urlResponse requestResult:requestResult operationContext:operationContext];
        if (error)
        {
            return error;
        }
        
        [AZSCloudBlob updateEtagAndLastModifiedWithResponse:urlResponse properties:self.properties updateLength:NO];
        return nil;
    }];
    
    [AZSExecutor ExecuteWithStorageCommand:command requestOptions:modifiedOptions operationContext:operationContext completionHandler:^(NSError *error, id result)
     {
         completionHandler(error);
     }];
}

-(void)appendBlockWithData:(NSData *)blockData contentMD5:(NSString *)contentMD5 completionHandler:(void (^)(NSError * __AZSNullable, NSNumber *appendOffset))completionHandler
{
    [self appendBlockWithData:blockData contentMD5:contentMD5 accessCondition:nil requestOptions:nil operationContext:nil completionHandler:completionHandler];
}

-(void)appendBlockWithData:(NSData *)blockData contentMD5:(NSString *)contentMD5 accessCondition:(AZSNullable AZSAccessCondition *)accessCondition requestOptions:(AZSNullable AZSBlobRequestOptions *)requestOptions operationContext:(AZSNullable AZSOperationContext *)operationContext completionHandler:(void (^)(NSError * __AZSNullable, NSNumber *appendOffset))completionHandler
{
    if (!operationContext)
    {
        operationContext = [[AZSOperationContext alloc] init];
    }
    AZSBlobRequestOptions *modifiedOptions = [[AZSBlobRequestOptions copyOptions:requestOptions] applyDefaultsFromOptions:self.client.defaultRequestOptions];
    AZSStorageCommand * command = [[AZSStorageCommand alloc] initWithStorageCredentials:self.client.credentials storageUri:self.storageUri operationContext:operationContext];
    
    if (requestOptions.useTransactionalMD5 && !(contentMD5))
    {
        unsigned char md5Bytes[CC_MD5_DIGEST_LENGTH];
        CC_MD5(blockData.bytes, (CC_LONG) blockData.length, md5Bytes);
        contentMD5 = [[[NSData alloc] initWithBytes:md5Bytes length:CC_MD5_DIGEST_LENGTH] base64EncodedStringWithOptions:0];
    }

    [command setBuildRequest:^ NSMutableURLRequest * (NSURLComponents *urlComponents, NSTimeInterval timeout, AZSOperationContext *operationContext)
     {
         return [AZSBlobRequestFactory appendBlockWithLength:blockData.length contentMD5:contentMD5 accessCondition:accessCondition urlComponents:urlComponents timeout:timeout operationContext:operationContext];
     }];
    
    [command setAuthenticationHandler:self.client.authenticationHandler];
    
    __block NSNumber *appendPosition;
    [command setPreProcessResponse:^id(NSHTTPURLResponse * urlResponse, AZSRequestResult * requestResult, AZSOperationContext * operationContext) {
        NSError *error = [AZSResponseParser preprocessResponseWithResponse:urlResponse requestResult:requestResult operationContext:operationContext];
        if (error)
        {
            return error;
        }
        
        [AZSCloudBlob updateEtagAndLastModifiedWithResponse:urlResponse properties:self.properties updateLength:NO];
        self.properties.appendBlobCommittedBlockCount = [AZSBlobResponseParser getAppendCommittedBlockCountWithResponse:urlResponse];
        appendPosition = [AZSBlobResponseParser getAppendPositionWithResponse:urlResponse];
        self.properties.length = [NSNumber numberWithLongLong:appendPosition.longLongValue + blockData.length];
        return nil;
    }];
    
    [command setSource:blockData];
    
    [AZSExecutor ExecuteWithStorageCommand:command requestOptions:modifiedOptions operationContext:operationContext completionHandler:^(NSError *error, id result)
     {
         completionHandler(error, appendPosition);
     }];
}

@end
