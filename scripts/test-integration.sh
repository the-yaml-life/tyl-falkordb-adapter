#!/bin/bash
# Integration test runner script for tyl-falkordb-adapter
# 
# This script sets up FalkorDB via docker-compose and runs integration tests

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
COMPOSE_FILE="docker-compose.test.yml"
TEST_TIMEOUT=300
CLEANUP_ON_EXIT=true
VERBOSE=false

# Function to print colored output
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to cleanup on exit
cleanup() {
    if [ "$CLEANUP_ON_EXIT" = true ]; then
        log_info "Cleaning up Docker containers..."
        $DOCKER_COMPOSE -f "$COMPOSE_FILE" down --volumes --remove-orphans 2>/dev/null || true
    fi
}

# Set trap for cleanup
trap cleanup EXIT

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --no-cleanup)
            CLEANUP_ON_EXIT=false
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --timeout)
            TEST_TIMEOUT="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --no-cleanup    Don't cleanup Docker containers after tests"
            echo "  --verbose       Show detailed output"
            echo "  --timeout N     Set test timeout in seconds (default: 300)"
            echo "  --help          Show this help message"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check if Docker is available and supports compose
if ! command -v docker &> /dev/null; then
    log_error "Docker is not installed or not in PATH"
    exit 1
fi

# Check for docker compose (v2) or docker-compose (v1)
if docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
elif command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
else
    log_error "Neither 'docker compose' nor 'docker-compose' is available"
    exit 1
fi

log_info "Using: $DOCKER_COMPOSE"

# Check if compose file exists
if [ ! -f "$COMPOSE_FILE" ]; then
    log_error "Compose file $COMPOSE_FILE not found"
    exit 1
fi

log_info "Starting FalkorDB integration test environment..."

# Start FalkorDB services
log_info "Starting FalkorDB services..."
if [ "$VERBOSE" = true ]; then
    $DOCKER_COMPOSE -f "$COMPOSE_FILE" up -d
else
    $DOCKER_COMPOSE -f "$COMPOSE_FILE" up -d > /dev/null 2>&1
fi

# Wait for services to be healthy
log_info "Waiting for FalkorDB to be ready..."
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if $DOCKER_COMPOSE -f "$COMPOSE_FILE" ps --format "table {{.Name}}\t{{.Status}}" | grep -q "Up (healthy)"; then
        log_success "FalkorDB is ready!"
        break
    fi
    
    attempt=$((attempt + 1))
    if [ $attempt -eq $max_attempts ]; then
        log_error "FalkorDB failed to start within expected time"
        $DOCKER_COMPOSE -f "$COMPOSE_FILE" logs falkordb-test
        exit 1
    fi
    
    log_info "Waiting for FalkorDB... (attempt $attempt/$max_attempts)"
    sleep 2
done

# Set environment variables for tests
export FALKORDB_TEST_URL="redis://localhost:6379"
export FALKORDB_PARALLEL_TEST_URL="redis://localhost:6380"
export RUST_LOG="info"
export RUST_BACKTRACE="1"

log_info "Running integration tests..."

# Run different types of tests
test_exit_code=0

# 1. Mock compatibility tests (should always work)
log_info "Running mock compatibility tests..."
if timeout "$TEST_TIMEOUT" cargo test --test integration_tests test_mock_compatibility; then
    log_success "Mock compatibility tests passed"
else
    log_error "Mock compatibility tests failed"
    test_exit_code=1
fi

# 2. FalkorDB integration tests (require Docker)
log_info "Running FalkorDB integration tests..."
if timeout "$TEST_TIMEOUT" cargo test --test integration_tests test_falkordb_integration; then
    log_success "FalkorDB integration tests passed"
else
    log_error "FalkorDB integration tests failed"
    test_exit_code=1
fi

# 3. Full end-to-end tests
log_info "Running end-to-end integration tests..."
if timeout "$TEST_TIMEOUT" cargo test --test integration_tests --verbose; then
    log_success "All integration tests passed"
else
    log_error "Some integration tests failed"
    test_exit_code=1
fi

# Show container status
if [ "$VERBOSE" = true ]; then
    log_info "Container status:"
    $DOCKER_COMPOSE -f "$COMPOSE_FILE" ps
fi

if [ $test_exit_code -eq 0 ]; then
    log_success "All integration tests completed successfully!"
else
    log_error "Some integration tests failed (exit code: $test_exit_code)"
fi

exit $test_exit_code