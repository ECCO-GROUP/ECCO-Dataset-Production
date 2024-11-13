colors = {'blue': '\u001b[36m',
          'red': '\u001b[31m',
          'green': '\u001b[32m',
          'end': '\u001b[0m'}

def printc(value, color_type, end='\n'):
    color = colors[color_type]
    end_color = colors['end']
    print(f'{color}{value}{end_color}', end=end)