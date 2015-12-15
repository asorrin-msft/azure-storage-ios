// -----------------------------------------------------------------------------------------
// <copyright file="AZSCloudBlobDirectoryTests.m" company="Microsoft">
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

#import <XCTest/XCTest.h>
#import "AZSCloudBlobContainer.h"
#import "AZSCloudBlobClient.h"
#import "AZSCloudBlobDirectory.h"
#import "AZSStorageUri.h"
#import "AZSBlobTestBase.h"
#import "AZSResultSegment.h"
#import "AZSCloudBlockBlob.h"
#import "AZSUtil.h"
#import "AZSTestSemaphore.h"

@interface AZSCloudBlobDirectoryTests : AZSBlobTestBase
@property NSString *containerName;
@property AZSCloudBlobContainer *blobContainer;

@end

@implementation AZSCloudBlobDirectoryTests

- (void)setUp
{
    [super setUp];
    self.containerName = [[NSString stringWithFormat:@"sampleioscontainer%@",[[[NSUUID UUID] UUIDString] stringByReplacingOccurrencesOfString:@"-" withString:@""]] lowercaseString];
    self.blobContainer = [self.blobClient containerReferenceFromName:self.containerName];
    AZSTestSemaphore *semaphore = [[AZSTestSemaphore alloc] init];
    
    [self.blobContainer createContainerWithCompletionHandler:^(NSError * error) {
        XCTAssertNil(error, @"Error in test setup, in creating container.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
        [semaphore signal];
    }];
    
    [semaphore wait];
    // Put setup code here; it will be run once, before the first test case.
}

- (void)tearDown
{
    // Put teardown code here; it will be run once, after the last test case.
    AZSCloudBlobContainer *blobContainer = [self.blobClient containerReferenceFromName:self.containerName];
    AZSTestSemaphore *semaphore = [[AZSTestSemaphore alloc] init];
    
    @try {
        // Best-effort cleanup
        // TODO: Change to delete if exists once that's implemented.
        
        [blobContainer deleteContainerWithCompletionHandler:^(NSError * error) {
            [semaphore signal];
        }];
    }
    @catch (NSException *exception) {
        
    }
    [semaphore wait];
    [super tearDown];
}

- (void)testBlobDirectoryInit
{
    AZSCloudBlobDirectory *directory = [[AZSCloudBlobDirectory alloc] initWithDirectoryName:@"dirName" container:self.blobContainer];
    XCTAssertEqualObjects(@"dirName/", directory.name, @"Directory names do not match.");
    XCTAssertEqual(self.blobContainer, directory.blobContainer, @"Containers do not match.");
    XCTAssertEqual(self.blobClient, directory.client, @"Blob clients do not match.");
    XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:self.blobClient.directoryDelimiter, self.blobContainer.storageUri.primaryUri.absoluteString, @"dirName", nil]), directory.storageUri.primaryUri.absoluteString, @"StorageURI does not match.");
    
    directory = [[AZSCloudBlobDirectory alloc] initWithDirectoryName:@"/" container:self.blobContainer];
    XCTAssertEqualObjects(@"/", directory.name, @"Directory names do not match.");
    XCTAssertEqual(self.blobContainer, directory.blobContainer, @"Containers do not match.");
    XCTAssertEqual(self.blobClient, directory.client, @"Blob clients do not match.");
    XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:NO withDelimiter:self.blobClient.directoryDelimiter, self.blobContainer.storageUri.primaryUri.absoluteString, @"/", nil]), directory.storageUri.primaryUri.absoluteString, @"StorageURI does not match.");
    
    directory = [[AZSCloudBlobDirectory alloc] initWithDirectoryName:@"" container:self.blobContainer];
    XCTAssertEqualObjects(@"", directory.name, @"Directory names do not match.");
    XCTAssertEqual(self.blobContainer, directory.blobContainer, @"Containers do not match.");
    XCTAssertEqual(self.blobClient, directory.client, @"Blob clients do not match.");
    XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:self.blobClient.directoryDelimiter, self.blobContainer.storageUri.primaryUri.absoluteString, nil]), directory.storageUri.primaryUri.absoluteString, @"StorageURI does not match.");
}

- (void)testBlobDirectoryNavigation
{
    AZSCloudBlobDirectory *directorya = [self.blobContainer directoryReferenceFromName:@"a"];
    AZSCloudBlobDirectory *directoryb = [self.blobContainer directoryReferenceFromName:@"b"];
    AZSCloudBlobDirectory *directoryac = [directorya subdirectoryReferenceFromName:@"c"];
    AZSCloudBlobDirectory *directoryacd = [directoryac subdirectoryReferenceFromName:@"d"];
    
    XCTAssertEqualObjects(@"a/", directorya.name, @"Directory names do not match.");
    XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:self.blobClient.directoryDelimiter, self.blobContainer.storageUri.primaryUri.absoluteString, @"a", nil]), directorya.storageUri.primaryUri.absoluteString, @"StorageURI does not match.");
    XCTAssertEqualObjects(@"b/", directoryb.name, @"Directory names do not match.");
    XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:self.blobClient.directoryDelimiter,self.blobContainer.storageUri.primaryUri.absoluteString,@"b", nil]), directoryb.storageUri.primaryUri.absoluteString, @"StorageURI does not match.");
    XCTAssertEqualObjects(@"a/c/", directoryac.name, @"Directory names do not match.");
    XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:self.blobClient.directoryDelimiter,self.blobContainer.storageUri.primaryUri.absoluteString,@"a",@"c", nil]), directoryac.storageUri.primaryUri.absoluteString, @"StorageURI does not match.");
    XCTAssertEqualObjects(@"a/c/d/", directoryacd.name, @"Directory names do not match.");
    XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:self.blobClient.directoryDelimiter,self.blobContainer.storageUri.primaryUri.absoluteString,@"a",@"c",@"d", nil]), directoryacd.storageUri.primaryUri.absoluteString, @"StorageURI does not match.");
    
    AZSCloudBlobDirectory *directoryacdParent = [directoryacd parentReference];
    AZSCloudBlobDirectory *directoryacdParentParent = [directoryacdParent parentReference];
    
    XCTAssertEqualObjects(@"a/c/", directoryacdParent.name, @"Directory names do not match.");
    XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:self.blobClient.directoryDelimiter,self.blobContainer.storageUri.primaryUri.absoluteString,@"a",@"c", nil]), directoryacdParent.storageUri.primaryUri.absoluteString, @"StorageURI does not match.");
    XCTAssertEqualObjects(@"a/", directoryacdParentParent.name, @"Directory names do not match.");
    XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:self.blobClient.directoryDelimiter,self.blobContainer.storageUri.primaryUri.absoluteString,@"a", nil]), directoryacdParentParent.storageUri.primaryUri.absoluteString, @"StorageURI does not match.");

    AZSCloudBlobDirectory *directoryaParent = [directorya parentReference];
    AZSCloudBlobDirectory *directoryacdParentParentParent = [directoryacdParentParent parentReference];
    XCTAssertEqualObjects(@"", directoryaParent.name, @"Nonexistent directory non empty.");
    XCTAssertEqualObjects(@"", directoryacdParentParentParent.name, @"Nonexistent directory non empty.");

    XCTAssertEqual(self.blobContainer, directorya.blobContainer, @"Incorrect blob container returned.");
    XCTAssertEqual(self.blobContainer, directoryb.blobContainer, @"Incorrect blob container returned.");
    XCTAssertEqual(self.blobContainer, directoryac.blobContainer, @"Incorrect blob container returned.");
    XCTAssertEqual(self.blobContainer, directoryacd.blobContainer, @"Incorrect blob container returned.");
    XCTAssertEqual(self.blobContainer, directoryacdParent.blobContainer, @"Incorrect blob container returned.");
    XCTAssertEqual(self.blobContainer, directoryacdParentParent.blobContainer, @"Incorrect blob container returned.");
}

- (void)testFlatListingInDirectory
{
    AZSTestSemaphore *semaphore = [[AZSTestSemaphore alloc] init];

    AZSCloudBlobDirectory *directorya = [self.blobContainer directoryReferenceFromName:@"a"];
    AZSCloudBlobDirectory *directoryb = [self.blobContainer directoryReferenceFromName:@"b"];
    AZSCloudBlobDirectory *directoryac = [directorya subdirectoryReferenceFromName:@"c"];
    AZSCloudBlobDirectory *directoryacd = [directoryac subdirectoryReferenceFromName:@"d"];
    
    NSString *blobaShortName = @"bloba";
    NSString *blobbShortName = @"blobb";
    NSString *blobacShortName = @"blobac";
    NSString *blobacdShortName = @"blobacd";
    
    AZSCloudBlockBlob *bloba = [directorya blockBlobReferenceFromName:blobaShortName];
    AZSCloudBlockBlob *blobb = [directoryb blockBlobReferenceFromName:blobbShortName];
    AZSCloudBlockBlob *blobac = [directoryac blockBlobReferenceFromName:blobacShortName];
    AZSCloudBlockBlob *blobacd = [directoryacd blockBlobReferenceFromName:blobacdShortName];
    
    [bloba uploadFromText:@"blobatext" completionHandler:^(NSError *error) {
        XCTAssertNil(error, @"Error in uploading blob.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
        [blobb uploadFromText:@"blobbtext:" completionHandler:^(NSError *error) {
            XCTAssertNil(error, @"Error in uploading blob.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
            [blobac uploadFromText:@"blobactext:" completionHandler:^(NSError *error) {
                XCTAssertNil(error, @"Error in uploading blob.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
                [blobacd uploadFromText:@"blobacdtext:" completionHandler:^(NSError *error) {
                    XCTAssertNil(error, @"Error in uploading blob.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
                    
                    NSMutableArray *blobArray = [NSMutableArray arrayWithCapacity:3];
                    NSMutableArray *directoryArray = [NSMutableArray arrayWithCapacity:0];
                    [self listAllInDirectoryOrContainer:directorya useFlatBlobListing:YES blobArrayToPopulate:blobArray directoryArrayToPopulate:directoryArray continuationToken:nil prefix:nil blobListingDetails:AZSBlobListingDetailsNone maxResults:5000 completionHandler:^(NSError *error) {
                        XCTAssertNil(error, @"Error in listing blobs.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
                        XCTAssertEqual(3, blobArray.count, @"Incorrect number of blobs listed.");
                        XCTAssertEqual(0, directoryArray.count, @"Incorrect number of directories listed.");
                        XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:NO withDelimiter:self.blobClient.directoryDelimiter,@"a",blobaShortName, nil]), ((AZSCloudBlob *)blobArray[0]).blobName, @"Blob names do not match.");
                        XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:NO withDelimiter:self.blobClient.directoryDelimiter,@"a",@"c",blobacShortName, nil]), ((AZSCloudBlob *)blobArray[1]).blobName, @"Blob names do not match.");
                        XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:NO withDelimiter:self.blobClient.directoryDelimiter,@"a",@"c",@"d",blobacdShortName, nil]), ((AZSCloudBlob *)blobArray[2]).blobName, @"Blob names do not match.");
                        
                        [semaphore signal];
                    }];
                }];
            }];
        }];
    }];
    
    [semaphore wait];
}

- (void)testNonFlatListingInDirectory
{
    AZSTestSemaphore *semaphore = [[AZSTestSemaphore alloc] init];
    
    AZSCloudBlobDirectory *directorya = [self.blobContainer directoryReferenceFromName:@"a"];
    AZSCloudBlobDirectory *directoryb = [self.blobContainer directoryReferenceFromName:@"b"];
    AZSCloudBlobDirectory *directoryac = [directorya subdirectoryReferenceFromName:@"c"];
    AZSCloudBlobDirectory *directoryacd = [directoryac subdirectoryReferenceFromName:@"d"];
    
    NSString *blobaShortName = @"bloba";
    NSString *blobbShortName = @"blobb";
    NSString *blobacShortName = @"blobac";
    NSString *blobacdShortName = @"blobacd";
    
    AZSCloudBlockBlob *bloba = [directorya blockBlobReferenceFromName:blobaShortName];
    AZSCloudBlockBlob *blobb = [directoryb blockBlobReferenceFromName:blobbShortName];
    AZSCloudBlockBlob *blobac = [directoryac blockBlobReferenceFromName:blobacShortName];
    AZSCloudBlockBlob *blobacd = [directoryacd blockBlobReferenceFromName:blobacdShortName];
    
    [bloba uploadFromText:@"blobatext" completionHandler:^(NSError *error) {
        XCTAssertNil(error, @"Error in uploading blob.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
        [blobb uploadFromText:@"blobbtext:" completionHandler:^(NSError *error) {
            XCTAssertNil(error, @"Error in uploading blob.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
            [blobac uploadFromText:@"blobactext:" completionHandler:^(NSError *error) {
                XCTAssertNil(error, @"Error in uploading blob.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
                [blobacd uploadFromText:@"blobacdtext:" completionHandler:^(NSError *error) {
                    XCTAssertNil(error, @"Error in uploading blob.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
                    
                    NSMutableArray *blobArray = [NSMutableArray arrayWithCapacity:1];
                    NSMutableArray *directoryArray = [NSMutableArray arrayWithCapacity:1];
                    [self listAllInDirectoryOrContainer:directorya useFlatBlobListing:NO blobArrayToPopulate:blobArray directoryArrayToPopulate:directoryArray continuationToken:nil prefix:nil blobListingDetails:AZSBlobListingDetailsNone maxResults:5000 completionHandler:^(NSError *error) {
                        XCTAssertNil(error, @"Error in listing blobs.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
                        XCTAssertEqual(1, blobArray.count, @"Incorrect number of blobs listed.");
                        XCTAssertEqual(1, directoryArray.count, @"Incorrect number of directories listed.");
                        XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:NO withDelimiter:self.blobClient.directoryDelimiter,@"a",blobaShortName, nil]), ((AZSCloudBlob *)blobArray[0]).blobName, @"Blob names do not match.");
                        XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:self.blobClient.directoryDelimiter,@"a",@"c", nil]), ((AZSCloudBlobDirectory *)directoryArray[0]).name, @"Directory names do not match.");
                        
                        [semaphore signal];
                    }];
                }];
            }];
        }];
    }];
    
    [semaphore wait];
}

- (void)testNonFlatListingInContainer
{
    AZSTestSemaphore *semaphore = [[AZSTestSemaphore alloc] init];
    
    AZSCloudBlobDirectory *directorya = [self.blobContainer directoryReferenceFromName:@"a"];
    AZSCloudBlobDirectory *directoryb = [self.blobContainer directoryReferenceFromName:@"b"];
    AZSCloudBlobDirectory *directoryac = [directorya subdirectoryReferenceFromName:@"c"];
    AZSCloudBlobDirectory *directoryacd = [directoryac subdirectoryReferenceFromName:@"d"];
    
    NSString *blobaShortName = @"bloba";
    NSString *blobbShortName = @"blobb";
    NSString *blobacShortName = @"blobac";
    NSString *blobacdShortName = @"blobacd";
    
    AZSCloudBlockBlob *bloba = [directorya blockBlobReferenceFromName:blobaShortName];
    AZSCloudBlockBlob *blobb = [directoryb blockBlobReferenceFromName:blobbShortName];
    AZSCloudBlockBlob *blobac = [directoryac blockBlobReferenceFromName:blobacShortName];
    AZSCloudBlockBlob *blobacd = [directoryacd blockBlobReferenceFromName:blobacdShortName];
    
    [bloba uploadFromText:@"blobatext" completionHandler:^(NSError *error) {
        XCTAssertNil(error, @"Error in uploading blob.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
        [blobb uploadFromText:@"blobbtext:" completionHandler:^(NSError *error) {
            XCTAssertNil(error, @"Error in uploading blob.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
            [blobac uploadFromText:@"blobactext:" completionHandler:^(NSError *error) {
                XCTAssertNil(error, @"Error in uploading blob.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
                [blobacd uploadFromText:@"blobacdtext:" completionHandler:^(NSError *error) {
                    XCTAssertNil(error, @"Error in uploading blob.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
                    
                    NSMutableArray *blobArray = [NSMutableArray arrayWithCapacity:0];
                    NSMutableArray *directoryArray = [NSMutableArray arrayWithCapacity:2];
                    [self listAllInDirectoryOrContainer:self.blobContainer useFlatBlobListing:NO blobArrayToPopulate:blobArray directoryArrayToPopulate:directoryArray continuationToken:nil prefix:nil blobListingDetails:AZSBlobListingDetailsNone maxResults:5000 completionHandler:^(NSError *error) {
                        XCTAssertNil(error, @"Error in listing blobs.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
                        XCTAssertEqual(0, blobArray.count, @"Incorrect number of blobs listed.");
                        XCTAssertEqual(2, directoryArray.count, @"Incorrect number of directories listed.");
                        XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:self.blobClient.directoryDelimiter,@"a", nil]), ((AZSCloudBlobDirectory *)directoryArray[0]).name, @"Directory names do not match.");
                        XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:self.blobClient.directoryDelimiter,@"b", nil]), ((AZSCloudBlobDirectory *)directoryArray[1]).name, @"Directory names do not match.");
                        
                        [semaphore signal];
                    }];
                }];
            }];
        }];
    }];
    
    [semaphore wait];
}

- (void)runTestCreatingBlobsInDirectoryWithDirectory:(AZSCloudBlobDirectory *)directory
{
    AZSTestSemaphore *semaphore = [[AZSTestSemaphore alloc] init];
    
    NSString *blobaShortName = @"bloba";
    AZSCloudBlockBlob *bloba = [directory blockBlobReferenceFromName:blobaShortName];
    NSString *blobText = @"blobText";
    
    [bloba uploadFromText:blobText completionHandler:^(NSError *error) {
        XCTAssertNil(error, @"Error in uploading blob.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
            
        AZSCloudBlockBlob *newBloba = [self.blobContainer blockBlobReferenceFromName:[directory.name stringByAppendingString:blobaShortName]];
        [newBloba downloadToTextWithCompletionHandler:^(NSError *error, NSString *newBlobText) {
            XCTAssertNil(error, @"Error in downloading blob.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
            XCTAssertEqualObjects(blobText, newBlobText, @"Blob text does not match.");
                
            NSString *blobbShortName = @"blobb";
            AZSCloudBlockBlob *blobb = [self.blobContainer blockBlobReferenceFromName:[directory.name stringByAppendingString:blobbShortName]];
            [blobb uploadFromText:blobText completionHandler:^(NSError *error) {
                XCTAssertNil(error, @"Error in uploading blob.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
                    
                AZSCloudBlockBlob *newBlobb = [directory blockBlobReferenceFromName:blobbShortName];
                [newBlobb downloadToTextWithCompletionHandler:^(NSError *error, NSString *newBlobText) {
                    XCTAssertNil(error, @"Error in downloading blob.  Error code = %ld, error domain = %@, error userinfo = %@", (long)error.code, error.domain, error.userInfo);
                    XCTAssertEqualObjects(blobText, newBlobText, @"Blob text does not match.");
                        
                    [semaphore signal];
                }];
            }];
        }];
    }];
    
    [semaphore wait];
}

- (void)testDifferentDelimiters
{
    
    NSArray *delimiters = [NSArray arrayWithObjects:@"xyz", @"$", @"@", @"-", @"%", @"/", @"|", @"xyz@$-%/|?", nil];
    
    for (NSString *delimiter in delimiters)
    {
        AZSCloudBlobClient *newClient = [[AZSCloudBlobClient alloc] initWithStorageUri:self.blobClient.storageUri credentials:self.blobClient.credentials];
        newClient.directoryDelimiter = delimiter;
        
        AZSCloudBlobContainer *container = [newClient containerReferenceFromName:self.containerName];
        AZSCloudBlobDirectory *directorya = [container directoryReferenceFromName:@"a"];
        AZSCloudBlobDirectory *directoryadelimInName = [container directoryReferenceFromName:[@"a" stringByAppendingString:delimiter]];
        AZSCloudBlobDirectory *directoryab = [directorya subdirectoryReferenceFromName:@"b"];
        AZSCloudBlobDirectory *directoryabc = [directoryab subdirectoryReferenceFromName:@"c"];
        AZSCloudBlobDirectory *directoryabcParent = [directoryabc parentReference];
        AZSCloudBlobDirectory *directoryabcParentParent = [directoryabcParent parentReference];
        AZSCloudBlockBlob *blobabc = [directoryabc blockBlobReferenceFromName:@"blobabc"];
    
        XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:delimiter,@"a",nil]), directorya.name, @"Directory names do not match.");
        XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:delimiter,@"a",nil]), directoryadelimInName.name, @"Directory names do not match.");
        XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:delimiter,@"a",@"b",nil]), directoryab.name, @"Directory names do not match.");
        XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:delimiter,@"a",@"b",@"c",nil]), directoryabc.name, @"Directory names do not match.");
        XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:delimiter,@"a",@"b",nil]), directoryabcParent.name, @"Directory names do not match.");
        XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:YES withDelimiter:delimiter,@"a",nil]), directoryabcParentParent.name, @"Directory names do not match.");

        XCTAssertEqualObjects(([self appendStringsAppendFinalDelimiter:NO withDelimiter:delimiter,@"a",@"b",@"c",@"blobabc",nil]), blobabc.blobName, @"Blob names do not match.");
    
        [self runTestCreatingBlobsInDirectoryWithDirectory:directoryabc];
    }
}

-(NSString *)appendStringsAppendFinalDelimiter:(BOOL)appendFinal withDelimiter:(NSString *)delimiter,...
{
    va_list args;
    va_start(args, delimiter);
    
    NSMutableString *builder = [NSMutableString string];
    NSString *firstArg = va_arg(args, NSString*);
    if (firstArg == nil)
    {
        return @"";
    }
    
    [builder appendString:firstArg];
    
    for (NSString *arg = va_arg(args, NSString*); arg != nil; arg = va_arg(args, NSString*))
    {
        [builder appendString:delimiter];
        [builder appendString:arg];
    }
    
    if (appendFinal)
    {
        [builder appendString:delimiter];
    }
    
    va_end(args);
    return builder;
}

-(void)listAllInDirectoryOrContainer:(NSObject *)objectToList useFlatBlobListing:(BOOL)useFlatBlobListing blobArrayToPopulate:(NSMutableArray *)blobArrayToPopulate directoryArrayToPopulate:(NSMutableArray *)directoryArrayToPopulate continuationToken:(AZSContinuationToken *)continuationToken prefix:(NSString *)prefix blobListingDetails:(AZSBlobListingDetails)blobListingDetails maxResults:(NSUInteger)maxResults completionHandler:(void (^)(NSError *))completionHandler
{
    void (^tempCompletion)(NSError *, AZSBlobResultSegment *) = ^void(NSError *error, AZSBlobResultSegment *results) {
        if (error)
        {
            completionHandler(error);
        }
        else
        {
            [blobArrayToPopulate addObjectsFromArray:results.blobs];
            [directoryArrayToPopulate addObjectsFromArray:results.directories];
            if (results.continuationToken)
            {
                [self listAllInDirectoryOrContainer:objectToList useFlatBlobListing:useFlatBlobListing blobArrayToPopulate:blobArrayToPopulate directoryArrayToPopulate:directoryArrayToPopulate continuationToken:results.continuationToken prefix:prefix blobListingDetails:blobListingDetails maxResults:maxResults completionHandler:completionHandler];
            }
            else
            {
                completionHandler(nil);
            }
        }
    };
    
    SEL selector = NSSelectorFromString(@"listBlobsSegmentedWithContinuationToken:prefix:useFlatBlobListing:blobListingDetails:maxResults:completionHandler:");

    if ([objectToList respondsToSelector:selector])
    {
        // It's a container
        AZSCloudBlobContainer *container = (AZSCloudBlobContainer *)objectToList;
        [container listBlobsSegmentedWithContinuationToken:continuationToken prefix:nil useFlatBlobListing:useFlatBlobListing blobListingDetails:blobListingDetails maxResults:maxResults completionHandler:tempCompletion];
    }
    else
    {
        // It's a directory
        AZSCloudBlobDirectory *directory = (AZSCloudBlobDirectory *)objectToList;
        [directory listBlobsSegmentedWithContinuationToken:continuationToken useFlatBlobListing:useFlatBlobListing blobListingDetails:blobListingDetails maxResults:maxResults completionHandler:tempCompletion];
    }
}

@end