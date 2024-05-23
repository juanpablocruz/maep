package gui

import (
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/juanpablocruz/maep/internal/maep"
)

type model struct {
	choices  []string
	cursor   int
	selected map[int]struct{}

	styles *Styles
	width  int
	lg     *lipgloss.Renderer

	tree *maep.Node
}

func initialModel(op *maep.Operation) model {
	var tree *maep.Node
	if op != nil {
		tree = maep.NewNode()
		ob, err := tree.AddOperation([]maep.Operation{*op})
		if err != nil {
			panic(err)
		}
		tree.AddLeaf(ob)
	}

	return model{
		choices:  []string{"Insert new operation", "Join network", "Sync"},
		cursor:   0,
		selected: make(map[int]struct{}),
		width:    maxWidth,
		lg:       lipgloss.DefaultRenderer(),
		styles:   NewStyles(lipgloss.DefaultRenderer()),
		tree:     tree,
	}
}

func (m model) Init() tea.Cmd {
	return nil
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctr+c", "q":
			return m, tea.Quit
		case "j", "down":
			if m.cursor < len(m.choices)-1 {
				m.cursor++
			}
		case "k", "up":
			if m.cursor > 0 {
				m.cursor--
			}
		case "enter":
			_, ok := m.selected[m.cursor]
			if ok {
				delete(m.selected, m.cursor)
			} else {
				m.selected[m.cursor] = struct{}{}

				if m.cursor == 0 {
					newOperationScren := initialNewOperationScreen()
					return RootScreen().SwitchScreen(&newOperationScren)
				}
			}
		}
	}
	return m, nil
}

func (m model) View() string {
	str := ""
	for i, choice := range m.choices {
		cursor := " "
		if i == m.cursor {
			cursor = ">"
		}
		selected := " "
		if _, ok := m.selected[i]; ok {
			selected = "x"
		}
		str += fmt.Sprintf("%s [%s] %s\n", cursor, selected, choice)
	}

	str += "\n\nPress q to quit\n"

	s := m.styles

	statusText := "No operations"
	if m.tree != nil && m.tree.Block != nil {
		statusText = m.tree.Block.GetShortHash()
	}
	const statusWidth = 40
	statusMarginLeft := m.width - statusWidth - lipgloss.Width(str) - s.Status.GetMarginRight()
	status := s.Status.
		Height(lipgloss.Height(statusText)).
		Width(statusWidth).
		MarginLeft(statusMarginLeft).
		Render(s.StatusHeader.Render("Status") + "\n" + statusText + "\n\n")

	header := RootScreen().appBoundaryView("Welcome to MAEP client")
	body := lipgloss.JoinHorizontal(lipgloss.Top, str, status)
	return s.Base.Render(header + "\n" + body + "\n\n")
}
