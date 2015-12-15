// -----------------------------------------------------------------------------------------
// <copyright file="AZSCloudBlobDirectory.h" company="Microsoft">
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

#import "AZSCloudBlobDirectory.h"
#import "AZSCloudBlobContainer.h"
#import "AZSCloudBlobClient.h"
#import "AZSStorageUri.h"
#import "AZSCloudBlockBlob.h"

@implementation AZSCloudBlobDirectory

- (instancetype)initWithDirectoryName:(NSString *)directoryName container:(AZSCloudBlobContainer *)container
{
    self = [super init];
    if (self)
    {
        NSRange lastDelimiterRange = [directoryName rangeOfString:container.client.directoryDelimiter options:NSBackwardsSearch];
        
        if ((directoryName.length == 0) || (lastDelimiterRange.location + lastDelimiterRange.length == directoryName.length))
        {
            _name = directoryName;
        }
        else
        {
            _name = [directoryName stringByAppendingString:container.client.directoryDelimiter];
        }
        _storageUri = [AZSStorageUri appendToStorageUri:container.storageUri pathToAppend:_name];
        _blobContainer = container;
    }
    
    return self;
}

- (AZSCloudBlobClient *)client
{
    return self.blobContainer.client;
}

- (AZSCloudBlockBlob *)blockBlobReferenceFromName:(NSString *)blobName snapshotTime:(NSString *)snapshotTime
{
    AZSCloudBlockBlob *blockBlob = [[AZSCloudBlockBlob alloc] initWithContainer:self.blobContainer name:[self.name stringByAppendingString:blobName] snapshotTime:snapshotTime];
    return blockBlob;
}

- (AZSCloudBlockBlob *)blockBlobReferenceFromName:(NSString *)blobName
{
    return [self blockBlobReferenceFromName:blobName snapshotTime:nil];
}

- (AZSCloudBlobDirectory *)subdirectoryReferenceFromName:(NSString *)subdirectoryName
{
    AZSCloudBlobDirectory *subDirectory = [[AZSCloudBlobDirectory alloc] initWithDirectoryName:[self.name stringByAppendingString:subdirectoryName] container:self.blobContainer];
    return subDirectory;
}

- (AZSCloudBlobDirectory *)parentReference
{
    NSRange lastRange = [self.name rangeOfString:self.blobContainer.client.directoryDelimiter options:NSBackwardsSearch];
    if (lastRange.location == NSNotFound)
    {
        return [[AZSCloudBlobDirectory alloc] initWithDirectoryName:AZSCEmptyString container:self.blobContainer];
    }
    else
    {
        NSString *parentDirectoryNameCandidate = [self.name substringToIndex:lastRange.location];
        NSRange secondRange = [parentDirectoryNameCandidate rangeOfString:self.blobContainer.client.directoryDelimiter options:NSBackwardsSearch];
        
        if (secondRange.location == NSNotFound)
        {
            return [[AZSCloudBlobDirectory alloc] initWithDirectoryName:AZSCEmptyString container:self.blobContainer];
        }
        else
        {
            return [[AZSCloudBlobDirectory alloc] initWithDirectoryName:[parentDirectoryNameCandidate substringToIndex:secondRange.location] container:self.blobContainer];
        }
    }
}

- (void)listBlobsSegmentedWithContinuationToken:(AZSNullable AZSContinuationToken *)token useFlatBlobListing:(BOOL)useFlatBlobListing blobListingDetails:(AZSBlobListingDetails)blobListingDetails maxResults:(NSInteger)maxResults accessCondition:(AZSNullable AZSAccessCondition *)accessCondition requestOptions:(AZSNullable AZSBlobRequestOptions *)requestOptions operationContext:(AZSNullable AZSOperationContext *)operationContext completionHandler:(void (^)(NSError * __AZSNullable, AZSBlobResultSegment * __AZSNullable))completionHandler
{
    [self.blobContainer listBlobsSegmentedWithContinuationToken:token prefix:self.name useFlatBlobListing:useFlatBlobListing blobListingDetails:blobListingDetails maxResults:maxResults accessCondition:accessCondition requestOptions:requestOptions operationContext:operationContext completionHandler:completionHandler];
}

- (void)listBlobsSegmentedWithContinuationToken:(AZSNullable AZSContinuationToken *)token useFlatBlobListing:(BOOL)useFlatBlobListing blobListingDetails:(AZSBlobListingDetails)blobListingDetails maxResults:(NSInteger)maxResults completionHandler:(void (^)(NSError * __AZSNullable, AZSBlobResultSegment * __AZSNullable))completionHandler;
{
    [self listBlobsSegmentedWithContinuationToken:token useFlatBlobListing:useFlatBlobListing blobListingDetails:blobListingDetails maxResults:maxResults accessCondition:nil requestOptions:nil operationContext:nil completionHandler:completionHandler];
}


@end
