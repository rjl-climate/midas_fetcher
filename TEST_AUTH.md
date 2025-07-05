# Testing Real CEDA Authentication

This guide explains how to test the real CEDA authentication functionality.

## Prerequisites

1. **CEDA Account**: You need a valid CEDA account from [https://services.ceda.ac.uk/](https://services.ceda.ac.uk/)
2. **Active Account**: Your account must be active and have access to MIDAS Open data

## Setup Instructions

### Option 1: Using .env file (Recommended)

1. Create a `.env` file in the project root:
   ```bash
   touch .env
   ```

2. Add your credentials to the `.env` file:
   ```env
   CEDA_USERNAME=your_username
   CEDA_PASSWORD=your_password
   ```

3. Set secure permissions (Unix/macOS):
   ```bash
   chmod 600 .env
   ```

### Option 2: Using environment variables

```bash
export CEDA_USERNAME=your_username
export CEDA_PASSWORD=your_password
```

## Running the Test

Run the authentication test with detailed output:

```bash
cargo test test_real_authentication -- --ignored --nocapture
```

## What the Test Does

1. **Loads Credentials**: Reads from `.env` file or environment variables
2. **Creates Authenticated Client**: Tests the full CEDA authentication flow:
   - Fetches login page
   - Extracts CSRF token
   - Submits login form
   - Verifies authentication with test download
3. **Downloads Test File**: Downloads a small MIDAS Open CSV file
4. **Validates Content**: Verifies the downloaded content is valid CSV data

## Expected Output

On success, you should see output like:
```
Loading credentials from .env file...
Testing CEDA authentication for user: your_username
Password length: XX characters
Creating CEDA client...
✅ Successfully created authenticated CEDA client
Testing file download...
Downloading test file: https://data.ceda.ac.uk/badc/ukmo-midas-open/...
✅ Successfully downloaded file to: /tmp/.../test_download.csv
📁 File size: XXXX bytes
📄 First few lines:
   1: Conventions,G,BADC-CSV,https://help.ceda.ac.uk/article/4982-midas-open-user-guide
   2: ob_end_time,id_type,id,ob_hour_count,version,met_domain_name,src_id,...
   3: 1980-01-02T06:00,DCNN,1381,24,1,ENGLAND_S,TWIST,50.94,-4.17,191,air_t...
✅ Successfully downloaded valid CSV file
📊 File contains XXXX lines
🎯 Detected BADC-CSV format
🎉 All tests passed! CEDA authentication and download working correctly.
```

## Troubleshooting

### Authentication Failed
```
❌ Failed to create CEDA client: CEDA login failed
💡 Check your username and password are correct
💡 Make sure your CEDA account is active
```
**Solution**: Verify your credentials and account status

### Network Errors
```
❌ Failed to create CEDA client: HTTP request failed during authentication
💡 Network error - check your internet connection
```
**Solution**: Check internet connectivity and CEDA service status

### Missing Credentials
```
CEDA_USERNAME environment variable not set. Please set credentials.
```
**Solution**: Set up credentials using one of the methods above

### Rate Limiting
```
❌ Download failed: Rate limit exceeded
💡 Rate limited - try again later
```
**Solution**: Wait a few minutes and try again

## Security Notes

- The `.env` file contains sensitive credentials - never commit it to version control
- The `.gitignore` file already excludes `.env` files
- File permissions are automatically set to 600 (owner read/write only) on Unix systems
- Credentials are only used for authentication testing

## File Downloaded

The test downloads a real MIDAS Open file:
- **Dataset**: UK Daily Temperature Observations
- **Station**: Twist, Devon (ID: 1381)
- **File**: Temperature data for 1980
- **Format**: BADC-CSV (British Atmospheric Data Centre CSV format)
- **Size**: Typically ~50KB with ~365 daily observations

This validates that:
1. Authentication is working correctly
2. You have access to MIDAS Open data
3. The HTTP client can download real files
4. File format detection is working

## Next Steps

Once this test passes, you can be confident that:
- CEDA authentication is working
- HTTP client with rate limiting is functional
- File download capabilities are operational
- The foundation is ready for building the full MIDAS Fetcher application