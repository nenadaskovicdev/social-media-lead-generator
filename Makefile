# Lead Generation Tool Makefile with Virtual Environment

.PHONY: help venv install install-dev env setup check-env run-web run-scraper clean all

# Virtual environment path
VENV_NAME = venv
VENV_BIN = $(VENV_NAME)/bin
PYTHON = $(VENV_BIN)/python
PIP = $(VENV_BIN)/pip

# Default target
help:
	@echo "Lead Generation Tool Makefile"
	@echo "============================="
	@echo "Available targets:"
	@echo "  make venv       - Create Python virtual environment"
	@echo "  make install    - Install production dependencies"
	@echo "  make install-dev - Install development dependencies"
	@echo "  make env        - Create .env file from template"
	@echo "  make setup      - Full setup (venv + env + install)"
	@echo "  make check-env  - Verify environment configuration"
	@echo "  make run-web    - Start the web application"
	@echo "  make run-scraper - Run the scraper (set COUNTRY=value)"
	@echo "  make all        - Run both web and scraper (in background)"
	@echo "  make clean      - Clean up temporary files and virtual environment"

# Create Python virtual environment
venv:
	@echo "Creating Python virtual environment..."
	python -m venv $(VENV_NAME)
	@echo "Virtual environment created at $(VENV_NAME)"

# Install production dependencies
install: venv
	@echo "Installing production dependencies..."
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "Installing Playwright browsers..."
	$(VENV_BIN)/playwright install
	@echo "Installation complete!"

# Install development dependencies
install-dev: venv
	@echo "Installing development dependencies..."
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install black flake8 isort pytest
	@echo "Installing Playwright browsers..."
	$(VENV_BIN)/playwright install
	@echo "Development installation complete!"

# Create environment file from template
env:
	@if [ ! -f .env ]; then \
		echo "Creating .env file from template..."; \
		cp .env.example .env; \
		echo "Please edit .env with your configuration"; \
	else \
		echo ".env file already exists"; \
	fi

# Full setup process
setup: venv install env
	@echo "Setup complete!"
	@echo "Virtual environment: $(VENV_NAME)"
	@echo "To activate: source $(VENV_NAME)/bin/activate"

# Check if virtual environment exists and is properly configured
check-venv:
	@if [ ! -d "$(VENV_NAME)" ]; then \
		echo "Error: Virtual environment not found. Run 'make venv' first."; \
		exit 1; \
	fi
	@if [ ! -f "$(VENV_BIN)/python" ]; then \
		echo "Error: Python not found in virtual environment. Run 'make install' first."; \
		exit 1; \
	fi

# Check environment configuration
check-env: check-venv
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found. Run 'make env' first."; \
		exit 1; \
	fi
	@echo "Environment file found"
	@echo "Virtual environment: $(VENV_NAME)"
	@echo "Current MongoDB URI: $$(grep MONGODB_URI .env | cut -d '=' -f2)"
	@echo "Current target country: $$(grep DEFAULT_COUNTRY .env | cut -d '=' -f2)"

# Run the web application
run-web: check-env
	@echo "Starting web application..."
	$(PYTHON) -m uvicorn web_app.main:app --reload --host 0.0.0.0 --port 8000

# Run the scraper (specify COUNTRY=value)
run-scraper: check-env
ifndef COUNTRY
	@echo "Error: COUNTRY variable not set. Usage: make run-scraper COUNTRY=france"
	@exit 1
endif
	@echo "Running scraper for country: $(COUNTRY)"
	$(PYTHON) -m scraper.main --country $(COUNTRY)

# Run both web and scraper (in background)
all: check-env
	@echo "Starting both web application and scraper..."
	@echo "Web UI will be available at: http://localhost:8000"
	@echo "Scraper will run for country: $$(grep DEFAULT_COUNTRY .env | cut -d '=' -f2)"
	@make run-web & 
	@sleep 5
	@make run-scraper COUNTRY=$$(grep DEFAULT_COUNTRY .env | cut -d '=' -f2)

# Clean up temporary files and virtual environment
clean:
	@echo "Cleaning up temporary files and virtual environment..."
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -delete
	find . -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf $(VENV_NAME)
	@echo "Clean complete. Run 'make setup' to recreate the environment."

# Create the .env.example file if it doesn't exist
.env.example:
	@echo "Creating .env.example file..."
	@echo "# Lead Generation Tool Environment Configuration" > .env.example
	@echo "# Copy this file to .env and update the values" >> .env.example
	@echo "" >> .env.example
	@echo "# MongoDB Connection" >> .env.example
	@echo "MONGODB_URI=mongodb://localhost:27017" >> .env.example
	@echo "DATABASE_NAME=lead_generation" >> .env.example
	@echo "COLLECTION_NAME=instagram_leads" >> .env.example
	@echo "" >> .env.example
	@echo "# Scraper Settings" >> .env.example
	@echo "REQUEST_DELAY_MS=2000" >> .env.example
	@echo "REQUESTS_PER_MINUTE=30" >> .env.example
	@echo "MAX_CONCURRENT_SESSIONS=3" >> .env.example
	@echo "PROXY_ENABLED=False" >> .env.example
	@echo "PROXY_LIST=" >> .env.example
	@echo "" >> .env.example
	@echo "# Application Settings" >> .env.example
	@echo "SCRAPER_KEYWORDS=designer,photographer,artist,entrepreneur" >> .env.example
	@echo "DEFAULT_COUNTRY=usa" >> .env.example

# Test if packages are installed correctly
test-install: check-venv
	@echo "Testing if required packages are installed..."
	$(PYTHON) -c "import motor; print('✓ motor installed')"
	$(PYTHON) -c "import playwright; print('✓ playwright installed')"
	$(PYTHON) -c "import fastapi; print('✓ fastapi installed')"
	$(PYTHON) -c "import uvicorn; print('✓ uvicorn installed')"
	@echo "All packages are installed correctly!"
