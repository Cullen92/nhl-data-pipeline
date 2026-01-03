"""
Generate an NHL rink image for Tableau background overlay.

This creates a clean SVG/PNG rink image that can be used as a background
in Tableau for shot location heatmaps.

Requires: matplotlib, numpy

Usage:
    python -m nhl_pipeline.export.generate_rink_image

Output:
    exports/nhl_rink_half.png  - Half rink (offensive zone) for heatmaps
    exports/nhl_rink_full.png  - Full rink image
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from pathlib import Path

# NHL Rink dimensions (in feet)
# Full rink: 200ft x 85ft
# We use half-rink for shot charts since shots are normalized
RINK_LENGTH = 200
RINK_WIDTH = 85
HALF_LENGTH = 100
HALF_WIDTH = 42.5

# Goal line is 11 feet from the end boards
GOAL_LINE_DISTANCE = 11

# Faceoff circles
FACEOFF_CIRCLE_RADIUS = 15

# Goal crease
CREASE_RADIUS = 6

EXPORT_DIR = Path(__file__).parent.parent.parent.parent / "exports"


def draw_half_rink(ax, color='#1e88e5', ice_color='white', line_color='red'):
    """Draw a half NHL rink (offensive zone) for shot heatmaps."""
    
    # Set background color (ice)
    ax.set_facecolor(ice_color)
    
    # Rink outline (half rink from center to end)
    # We're drawing the offensive half (0 to 100 in x, 0 to 42.5 in y)
    
    # Corner radius
    corner_radius = 28
    
    # Draw rink boundary with rounded corners at the far end
    # Left side
    ax.plot([0, 0], [0, HALF_WIDTH], color='black', linewidth=2)
    
    # Right side (with rounded corner at top)
    ax.plot([HALF_LENGTH - corner_radius, HALF_LENGTH - corner_radius], 
            [0, 0], color='black', linewidth=2, alpha=0)  # hidden
    
    # Top and bottom (straight sections)
    ax.plot([0, HALF_LENGTH - corner_radius], [HALF_WIDTH, HALF_WIDTH], 
            color='black', linewidth=2)
    ax.plot([0, HALF_LENGTH - corner_radius], [0, 0], 
            color='black', linewidth=2)
    
    # Rounded corners at far end
    corner_top = patches.Arc(
        (HALF_LENGTH - corner_radius, HALF_WIDTH - corner_radius),
        corner_radius * 2, corner_radius * 2,
        angle=0, theta1=0, theta2=90,
        color='black', linewidth=2
    )
    ax.add_patch(corner_top)
    
    corner_bottom = patches.Arc(
        (HALF_LENGTH - corner_radius, corner_radius),
        corner_radius * 2, corner_radius * 2,
        angle=0, theta1=270, theta2=360,
        color='black', linewidth=2
    )
    ax.add_patch(corner_bottom)
    
    # End boards (far end)
    ax.plot([HALF_LENGTH, HALF_LENGTH], 
            [corner_radius, HALF_WIDTH - corner_radius], 
            color='black', linewidth=2)
    
    # Center line (red) at x=0
    ax.axvline(x=0, color='red', linewidth=3, linestyle='-')
    
    # Blue line at x=25 (from center ice, blue line is 25ft into offensive zone)
    ax.axvline(x=25, color='blue', linewidth=3)
    
    # Goal line at x=89 (11ft from end boards: 100 - 11 = 89)
    goal_line_x = HALF_LENGTH - GOAL_LINE_DISTANCE
    ax.axvline(x=goal_line_x, color='red', linewidth=2)
    
    # Goal crease
    crease = patches.Arc(
        (goal_line_x, HALF_WIDTH / 2),
        CREASE_RADIUS * 2, CREASE_RADIUS * 2,
        angle=0, theta1=270, theta2=90,
        color='red', linewidth=2
    )
    ax.add_patch(crease)
    
    # Crease fill
    crease_fill = patches.Wedge(
        (goal_line_x, HALF_WIDTH / 2),
        CREASE_RADIUS,
        270, 90,
        color='lightblue', alpha=0.5
    )
    ax.add_patch(crease_fill)
    
    # Goal (net outline)
    goal_width = 6  # 6 feet wide
    goal_depth = 2  # depth into boards
    goal = patches.Rectangle(
        (goal_line_x, HALF_WIDTH / 2 - goal_width / 2),
        goal_depth, goal_width,
        linewidth=2, edgecolor='red', facecolor='none'
    )
    ax.add_patch(goal)
    
    # Faceoff circles in offensive zone
    # Two circles: one on each side, centered at x=69 (20ft from goal line)
    # and y = 22 and y = 63 (22ft from center of rink)
    circle_x = goal_line_x - 20  # 20 feet from goal line
    circle_y_offset = 22  # 22 feet from center
    
    for y_pos in [HALF_WIDTH / 2 - circle_y_offset + 22, HALF_WIDTH / 2 + circle_y_offset - 22]:
        # Faceoff circle
        circle = patches.Circle(
            (circle_x, y_pos if y_pos > HALF_WIDTH/2 else HALF_WIDTH - y_pos),
            FACEOFF_CIRCLE_RADIUS,
            fill=False, color='red', linewidth=1.5
        )
        ax.add_patch(circle)
        
        # Faceoff dot
        dot = patches.Circle(
            (circle_x, y_pos if y_pos > HALF_WIDTH/2 else HALF_WIDTH - y_pos),
            1,
            fill=True, color='red'
        )
        ax.add_patch(dot)
    
    # Corrected faceoff circles
    for y_pos in [20.5, 64.5 - HALF_WIDTH + 42.5]:
        if 0 < y_pos < HALF_WIDTH:
            circle = patches.Circle(
                (69, y_pos),
                FACEOFF_CIRCLE_RADIUS,
                fill=False, color='red', linewidth=1.5
            )
            ax.add_patch(circle)
            dot = patches.Circle(
                (69, y_pos),
                1,
                fill=True, color='red'
            )
            ax.add_patch(dot)
    
    # Neutral zone faceoff dots
    for y_pos in [20.5, HALF_WIDTH - 20.5]:
        dot = patches.Circle(
            (20, y_pos),
            1,
            fill=True, color='red'
        )
        ax.add_patch(dot)
    
    # Set limits
    ax.set_xlim(-5, HALF_LENGTH + 5)
    ax.set_ylim(-5, HALF_WIDTH + 5)
    ax.set_aspect('equal')
    ax.axis('off')


def draw_simple_half_rink(ax):
    """Draw a simplified half rink for cleaner heatmap visualization."""
    
    ax.set_facecolor('#f0f8ff')  # Light ice blue
    
    # Rink boundary
    ax.plot([0, 100], [0, 0], 'k-', linewidth=2)
    ax.plot([0, 100], [42.5, 42.5], 'k-', linewidth=2)
    ax.plot([0, 0], [0, 42.5], 'k-', linewidth=2)
    
    # Rounded end
    theta = np.linspace(-np.pi/2, np.pi/2, 50)
    corner_r = 20
    # Bottom corner
    ax.plot(100 - corner_r + corner_r * np.cos(theta[theta < 0]),
            corner_r + corner_r * np.sin(theta[theta < 0]), 'k-', linewidth=2)
    # Top corner  
    ax.plot(100 - corner_r + corner_r * np.cos(theta[theta > 0]),
            42.5 - corner_r + corner_r * np.sin(theta[theta > 0]), 'k-', linewidth=2)
    
    # Center line (red)
    ax.axvline(x=0, color='red', linewidth=4)
    
    # Blue line
    ax.axvline(x=25, color='blue', linewidth=3)
    
    # Goal line
    ax.axvline(x=89, color='red', linewidth=2)
    
    # Goal crease
    crease = patches.Wedge((89, 21.25), 6, 270, 90, color='lightblue', alpha=0.7)
    ax.add_patch(crease)
    crease_outline = patches.Arc((89, 21.25), 12, 12, theta1=270, theta2=90, 
                                  color='red', linewidth=1.5)
    ax.add_patch(crease_outline)
    
    # Goal net
    ax.add_patch(patches.Rectangle((89, 18.25), 2, 6, 
                                    facecolor='white', edgecolor='red', linewidth=2))
    
    # Faceoff circles (offensive zone)
    for y in [11, 31.5]:
        circle = patches.Circle((69, y), 15, fill=False, color='red', linewidth=1)
        ax.add_patch(circle)
        dot = patches.Circle((69, y), 0.75, color='red')
        ax.add_patch(dot)
    
    # Neutral zone dots
    for y in [11, 31.5]:
        dot = patches.Circle((20, y), 0.75, color='red')
        ax.add_patch(dot)
    
    ax.set_xlim(-2, 102)
    ax.set_ylim(-2, 44.5)
    ax.set_aspect('equal')
    ax.axis('off')


def main():
    """Generate rink images for Tableau."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Simple half rink (best for heatmaps)
    fig, ax = plt.subplots(figsize=(12, 6), dpi=150)
    draw_simple_half_rink(ax)
    plt.tight_layout()
    
    output_path = EXPORT_DIR / "nhl_rink_half.png"
    plt.savefig(output_path, dpi=150, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    plt.close()
    print(f"Saved: {output_path}")
    
    # Also save as transparent PNG for overlay
    fig, ax = plt.subplots(figsize=(12, 6), dpi=150)
    draw_simple_half_rink(ax)
    ax.set_facecolor('none')
    plt.tight_layout()
    
    output_path_transparent = EXPORT_DIR / "nhl_rink_half_transparent.png"
    plt.savefig(output_path_transparent, dpi=150, bbox_inches='tight',
                transparent=True, edgecolor='none')
    plt.close()
    print(f"Saved: {output_path_transparent}")
    
    print(f"\n{'='*60}")
    print("TABLEAU BACKGROUND IMAGE SETUP:")
    print("="*60)
    print("1. Go to Map → Background Images → Add Image")
    print(f"2. Select: {output_path}")
    print("3. Set X Field: x_bin")
    print("   - Left: -2")
    print("   - Right: 102")
    print("4. Set Y Field: y_bin")
    print("   - Bottom: -2")
    print("   - Top: 44.5")
    print("5. Check 'Lock Aspect Ratio'")
    print("="*60)


if __name__ == "__main__":
    main()
