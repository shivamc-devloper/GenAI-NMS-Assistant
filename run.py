#!/usr/bin/env python3
"""
GenAI Network Monitoring System - Start Both Backend and Frontend

This script starts both the backend (FastAPI) and frontend (React) servers
and provides a unified interface for managing both services.

Usage:
    python run.py              # Start both services
    python run.py --backend    # Start only backend
    python run.py --frontend   # Start only frontend
    python run.py --help       # Show help
"""

import os
import sys
import subprocess
import time
import signal
import argparse
from pathlib import Path
from typing import Optional, List

# Colors for terminal output
class Colors:
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

class ServiceManager:
    def __init__(self, backend_port: int = 8001, frontend_port: int = 5173):
        self.backend_port = backend_port
        self.frontend_port = frontend_port
        self.backend_process: Optional[subprocess.Popen] = None
        self.frontend_process: Optional[subprocess.Popen] = None
        self.project_root = Path(__file__).parent
        
    def print_header(self, text: str):
        """Print formatted header"""
        width = 60
        print(f"\n{Colors.BLUE}{'╔' + '═' * (width - 2) + '╗'}{Colors.RESET}")
        print(f"{Colors.BLUE}║  {text:<{width - 4}}  ║{Colors.RESET}")
        print(f"{Colors.BLUE}{'╚' + '═' * (width - 2) + '╝'}{Colors.RESET}\n")
    
    def print_success(self, text: str):
        """Print success message"""
        print(f"{Colors.GREEN}✓ {text}{Colors.RESET}")
    
    def print_error(self, text: str):
        """Print error message"""
        print(f"{Colors.RED}✗ {text}{Colors.RESET}")
    
    def print_warning(self, text: str):
        """Print warning message"""
        print(f"{Colors.YELLOW}⚠ {text}{Colors.RESET}")
    
    def print_info(self, text: str):
        """Print info message"""
        print(f"{Colors.YELLOW}ℹ {text}{Colors.RESET}")
    
    def check_prerequisites(self):
        """Check if Python and Node.js are available"""
        print(f"{Colors.YELLOW}Checking prerequisites...{Colors.RESET}")
        
        # Check Python
        try:
            result = subprocess.run(['python3', '--version'], 
                                  capture_output=True, text=True, timeout=5)
            self.print_success(f"Python found: {result.stdout.strip()}")
        except (subprocess.TimeoutExpired, FileNotFoundError):
            self.print_error("Python3 not found. Please install Python 3.9+")
            sys.exit(1)
        
        # Check Node.js
        try:
            result = subprocess.run(['node', '--version'], 
                                  capture_output=True, text=True, timeout=5)
            self.print_success(f"Node.js found: {result.stdout.strip()}")
        except (subprocess.TimeoutExpired, FileNotFoundError):
            self.print_error("Node.js not found. Please install Node.js 16+")
            sys.exit(1)
        
        print()
    
    def check_directories(self):
        """Check if backend and frontend directories exist"""
        print(f"{Colors.YELLOW}Checking directories...{Colors.RESET}")
        
        backend_dir = self.project_root / "backend"
        frontend_dir = self.project_root / "Frontend"
        
        if not backend_dir.exists():
            self.print_error(f"Backend directory not found: {backend_dir}")
            sys.exit(1)
        self.print_success(f"Backend directory found")
        
        if not frontend_dir.exists():
            self.print_error(f"Frontend directory not found: {frontend_dir}")
            sys.exit(1)
        self.print_success(f"Frontend directory found")
        
        print()
    
    def install_backend_deps(self):
        """Install backend dependencies if needed"""
        backend_dir = self.project_root / "backend"
        
        try:
            subprocess.run(['python3', '-c', 'import fastapi'], 
                         capture_output=True, timeout=5)
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
            print(f"{Colors.YELLOW}Installing backend dependencies...{Colors.RESET}")
            subprocess.run(['pip', 'install', '-r', 'requirements.txt'],
                         cwd=backend_dir, capture_output=True)
            self.print_success("Backend dependencies installed")
    
    def install_frontend_deps(self):
        """Install frontend dependencies if needed"""
        frontend_dir = self.project_root / "Frontend"
        node_modules = frontend_dir / "node_modules"
        
        if not node_modules.exists():
            print(f"{Colors.YELLOW}Installing frontend dependencies...{Colors.RESET}")
            subprocess.run(['npm', 'install'], cwd=frontend_dir, capture_output=True)
            self.print_success("Frontend dependencies installed")
    
    def start_backend(self):
        """Start the backend server"""
        print(f"{Colors.YELLOW}Starting Backend (FastAPI)...{Colors.RESET}")
        
        backend_dir = self.project_root / "backend"
        
        try:
            self.backend_process = subprocess.Popen(
                ['python3', '-m', 'uvicorn', 'app.main:app', 
                 '--reload', '--port', str(self.backend_port)],
                cwd=backend_dir,
                text=True
            )
            self.print_success(f"Backend started (PID: {self.backend_process.pid})")
            print(f"  URL: http://localhost:{self.backend_port}")
            print(f"  API Docs: http://localhost:{self.backend_port}/docs\n")
            
            # Wait for backend to be ready
            self._wait_for_service(f"http://localhost:{self.backend_port}/health", "Backend")
            
        except Exception as e:
            self.print_error(f"Failed to start backend: {e}")
            sys.exit(1)
    
    def start_frontend(self):
        """Start the frontend server"""
        print(f"{Colors.YELLOW}Starting Frontend (React)...{Colors.RESET}")
        
        frontend_dir = self.project_root / "Frontend"
        
        try:
            self.frontend_process = subprocess.Popen(
                ['npm', 'run', 'dev'],
                cwd=frontend_dir,
                text=True
            )
            self.print_success(f"Frontend started (PID: {self.frontend_process.pid})")
            print(f"  URL: http://localhost:{self.frontend_port}\n")
            
            # Wait for frontend to be ready
            self._wait_for_service(f"http://localhost:{self.frontend_port}", "Frontend")
            
        except Exception as e:
            self.print_error(f"Failed to start frontend: {e}")
            sys.exit(1)
    
    def _wait_for_service(self, url: str, service_name: str, timeout: int = 30):
        """Wait for a service to be ready"""
        print(f"{Colors.YELLOW}Waiting for {service_name} to be ready...{Colors.RESET}")
        
        try:
            import urllib.request
            for i in range(timeout):
                try:
                    urllib.request.urlopen(url, timeout=2)
                    self.print_success(f"{service_name} is ready")
                    return
                except Exception:
                    if i < timeout - 1:
                        time.sleep(1)
            
            self.print_warning(f"{service_name} may still be starting...")
        except ImportError:
            time.sleep(3)
            self.print_info(f"Assuming {service_name} is ready")
    
    def display_summary(self):
        """Display service summary"""
        self.print_header("SERVICES RUNNING")
        
        print(f"{Colors.GREEN}✓ Backend {Colors.RESET} : http://localhost:{self.backend_port}")
        print(f"{Colors.GREEN}✓ Frontend{Colors.RESET} : http://localhost:{self.frontend_port}\n")
        
        print(f"{Colors.YELLOW}API Documentation{Colors.RESET}: http://localhost:{self.backend_port}/docs\n")
        
        print(f"{Colors.YELLOW}Press Ctrl+C to stop all services{Colors.RESET}\n")
    
    def cleanup(self, signum=None, frame=None):
        """Cleanup and stop all services"""
        print(f"\n{Colors.YELLOW}Shutting down services...{Colors.RESET}")
        
        if self.backend_process:
            try:
                self.backend_process.terminate()
                self.backend_process.wait(timeout=5)
                self.print_success("Backend stopped")
            except Exception:
                self.backend_process.kill()
                self.print_success("Backend stopped (forced)")
        
        if self.frontend_process:
            try:
                self.frontend_process.terminate()
                self.frontend_process.wait(timeout=5)
                self.print_success("Frontend stopped")
            except Exception:
                self.frontend_process.kill()
                self.print_success("Frontend stopped (forced)")
        
        print(f"{Colors.BLUE}Goodbye!{Colors.RESET}\n")
        sys.exit(0)
    
    def run_all(self):
        """Run both backend and frontend"""
        self.print_header("GenAI Network Monitoring System - Start Script")
        
        self.check_prerequisites()
        self.check_directories()
        
        print(f"{Colors.YELLOW}Setting up services...{Colors.RESET}\n")
        self.install_backend_deps()
        self.install_frontend_deps()
        print()
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self.cleanup)
        signal.signal(signal.SIGTERM, self.cleanup)
        
        # Start services
        self.start_backend()
        self.start_frontend()
        
        # Display summary
        self.display_summary()
        
        # Keep running
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.cleanup()
    
    def run_backend_only(self):
        """Run only the backend"""
        self.print_header("GenAI Network Monitoring System - Backend Only")
        
        self.check_prerequisites()
        self.check_directories()
        self.install_backend_deps()
        
        signal.signal(signal.SIGINT, self.cleanup)
        signal.signal(signal.SIGTERM, self.cleanup)
        
        self.start_backend()
        
        print(f"{Colors.YELLOW}Press Ctrl+C to stop{Colors.RESET}\n")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.cleanup()
    
    def run_frontend_only(self):
        """Run only the frontend"""
        self.print_header("GenAI Network Monitoring System - Frontend Only")
        
        self.check_prerequisites()
        self.check_directories()
        self.install_frontend_deps()
        
        signal.signal(signal.SIGINT, self.cleanup)
        signal.signal(signal.SIGTERM, self.cleanup)
        
        self.start_frontend()
        
        print(f"{Colors.YELLOW}Press Ctrl+C to stop{Colors.RESET}\n")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.cleanup()


def main():
    parser = argparse.ArgumentParser(
        description='GenAI Network Monitoring System - Start Script',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python run.py              # Start both backend and frontend
  python run.py --backend    # Start only backend
  python run.py --frontend   # Start only frontend
        '''
    )
    
    parser.add_argument('--backend', action='store_true', 
                       help='Start only backend')
    parser.add_argument('--frontend', action='store_true', 
                       help='Start only frontend')
    parser.add_argument('--backend-port', type=int, default=8001,
                       help='Backend port (default: 8001)')
    parser.add_argument('--frontend-port', type=int, default=5173,
                       help='Frontend port (default: 5173)')
    
    args = parser.parse_args()
    
    manager = ServiceManager(
        backend_port=args.backend_port,
        frontend_port=args.frontend_port
    )
    
    if args.backend:
        manager.run_backend_only()
    elif args.frontend:
        manager.run_frontend_only()
    else:
        manager.run_all()


if __name__ == '__main__':
    main()
