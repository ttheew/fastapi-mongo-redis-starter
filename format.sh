#!/bin/bash
set -e

echo "🧹 Running autoflake..."
autoflake --in-place --remove-all-unused-imports --remove-unused-variables -r .

echo "📚 Running isort..."
isort .

echo "🎨 Running black..."
black .

echo "✅ Codebase cleaned, organized, and formatted!"
