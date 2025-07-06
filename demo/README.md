# plugz Demo CLI

Interactive demonstrations of plugz capabilities with beautiful colored output.

## Building

```bash
cd demo
go mod tidy
go build -o plugz-demo
```

## Running

```bash
# Show help
./plugz-demo --help

# Run all demos
./plugz-demo all

# Run individual demos
./plugz-demo security    # Security audit pipeline
./plugz-demo transform   # Data transformation  
./plugz-demo universes   # Multi-tenant type universes
```

## Features

- 🎨 Colored terminal output
- 📊 Live code demonstrations
- 🔍 Interactive examples
- 📖 Syntax-highlighted code samples

## Troubleshooting

If colors don't appear:
- Make sure your terminal supports ANSI colors
- Try setting: `export FORCE_COLOR=1`
- On Windows, use Windows Terminal or similar

If formatting looks wrong:
- Ensure terminal width is at least 80 characters
- Use a monospace font in your terminal